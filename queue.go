package shoveler

import (
	"container/list"

	"github.com/joncrlsn/dque"

	"errors"
	"fmt"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"sync"
	"time"
)

type MessageStruct struct {
	Message  []byte
	Exchange string // Optional: specific exchange for this message (empty = use default)
}

type ConfirmationQueue struct {
	diskQueue *dque.DQue
	mutex     sync.Mutex
	emptyCond *sync.Cond
	memQueue  *list.List
	usingDisk bool
}

var (
	ErrEmpty     = errors.New("queue is empty")
	MaxInMemory  = 100
	LowWaterMark = 50
)

// maxCorruptSegments bounds how many corrupt segments are moved aside during a
// single startup. A queue directory that keeps producing corruption after this
// many is not recoverable one file at a time, so we stop and surface the error
// rather than deleting the whole spool a segment at a time.
const maxCorruptSegments = 10

// corruptSegmentPattern matches the segment path in a dque load error, e.g.
// "segment file /var/spool/.../0000000000988.dque is corrupted: ...".
// dque formats this with a single space (segment.go: "segment file %s is
// corrupted: %s"), but the whitespace is matched loosely so the pattern still
// holds if that text is ever reflowed or reformatted upstream.
var corruptSegmentPattern = regexp.MustCompile(`segment file (\S+\.dque)\s+is corrupted`)

// corruptSegmentPath extracts the corrupt segment path from a dque error, but
// only when the file really sits inside queueDir. A path from anywhere else is
// treated as no match: the error text is not something we want to turn into an
// unbounded "rename any file on the box" primitive.
func corruptSegmentPath(errMsg, queueDir string) string {
	match := corruptSegmentPattern.FindStringSubmatch(errMsg)
	if match == nil {
		return ""
	}

	segment := filepath.Clean(match[1])
	dir := filepath.Clean(queueDir)
	if filepath.Dir(segment) != dir {
		return ""
	}

	return segment
}

// quarantineSegment renames a corrupt segment out of the way so the queue can be
// reopened without it. The file is kept (not deleted) next to the queue with a
// .corrupt-<timestamp> suffix, so the undelivered messages can be inspected or
// recovered by hand afterwards.
func quarantineSegment(segment string) (string, error) {
	target := fmt.Sprintf("%s.corrupt-%s", segment, time.Now().UTC().Format("20060102T150405Z"))
	if err := os.Rename(segment, target); err != nil {
		return "", err
	}
	return target, nil
}

// openQueue opens the on-disk queue, moving corrupt segments aside and retrying
// rather than failing outright.
//
// A segment truncated by an abrupt process exit (or by anything pruning the
// queue directory) makes dque.NewOrOpen fail forever: the collector panicked at
// startup, systemd restarted it, and it panicked again on the same file, so a
// single bad segment took the collector down permanently and every UDP packet
// arriving meanwhile was lost. Losing the messages in one segment is bad;
// losing the whole collector until someone notices is much worse.
func openQueue(qName, qDir, queueDir string, segmentSize int) (*dque.DQue, error) {
	for attempt := 0; ; attempt++ {
		queue, err := dque.NewOrOpen(qName, qDir, segmentSize, ItemBuilder)
		if err == nil {
			return queue, nil
		}

		if attempt >= maxCorruptSegments {
			return nil, fmt.Errorf("gave up after quarantining %d corrupt segments: %w", attempt, err)
		}

		segment := corruptSegmentPath(err.Error(), queueDir)
		if segment == "" {
			// Not a corrupt-segment failure (bad permissions, missing dir, ...);
			// nothing to quarantine, so report it unchanged.
			return nil, err
		}

		target, renameErr := quarantineSegment(segment)
		if renameErr != nil {
			return nil, fmt.Errorf("failed to quarantine corrupt segment %s after open error %v: %w", segment, err, renameErr)
		}

		QueueSegmentsQuarantined.Inc()
		log.Errorf("Corrupt queue segment %s moved to %s; the messages it held will not be delivered, but the file is kept there for inspection. Continuing with the rest of the queue", segment, target)
	}
}

// NewConfirmationQueue returns an initialized list.
func NewConfirmationQueue(config *Config) *ConfirmationQueue {
	return new(ConfirmationQueue).Init(config)
}

// ItemBuilder creates a new item and returns a pointer to it.
// This is used when we load a segment of the queue from disk.
func ItemBuilder() interface{} {
	return &MessageStruct{}
}

// Init initializes the queue
func (cq *ConfirmationQueue) Init(config *Config) *ConfirmationQueue {
	qName := path.Base(config.QueueDir)
	qDir := path.Dir(config.QueueDir)
	segmentSize := 10000
	var err error
	cq.diskQueue, err = openQueue(qName, qDir, config.QueueDir, segmentSize)
	if err != nil {
		log.Panicln("Failed to create queue:", err)
	}
	err = cq.diskQueue.TurboOn()
	if err != nil {
		log.Errorln("Failed to turn on dque Turbo mode, the queue will be safer but much slower:", err)
	}

	// Check if we have any messages in the queue
	if cq.diskQueue.Size() > 0 {
		cq.usingDisk = true
	}

	cq.emptyCond = sync.NewCond(&cq.mutex)

	// Start the metrics goroutine
	cq.memQueue = list.New()
	go cq.queueMetrics()
	return cq

}

func (cq *ConfirmationQueue) Size() int {
	cq.mutex.Lock()
	defer cq.mutex.Unlock()
	if cq.usingDisk {
		return cq.diskQueue.SizeUnsafe()
	} else {
		return cq.memQueue.Len()
	}
}

// queueMetrics updates the queue size prometheus metric
// Should be run within a go routine
func (cq *ConfirmationQueue) queueMetrics() {
	// Setup the timer, every 5 seconds update the queue
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	// Do a select on the timer
	for {
		<-ticker.C
		// Update the prometheus
		queueSizeInt := cq.Size()
		QueueSize.Set(float64(queueSizeInt))
		log.Debugln("Queue Size:", queueSizeInt)

	}

}

// Enqueue the message
func (cq *ConfirmationQueue) Enqueue(msg []byte) {
	cq.EnqueueToExchange(msg, "")
}

// EnqueueToExchange enqueues a message with an optional specific exchange
// If exchange is empty, the default exchange from config will be used
func (cq *ConfirmationQueue) EnqueueToExchange(msg []byte, exchange string) {
	cq.mutex.Lock()
	defer cq.mutex.Unlock()

	msgStruct := &MessageStruct{Message: msg, Exchange: exchange}

	// Still using in-memory
	if !cq.usingDisk && (cq.memQueue.Len()+1) < MaxInMemory {
		cq.memQueue.PushBack(msgStruct)
	} else if !cq.usingDisk && (cq.memQueue.Len()+1) >= MaxInMemory {
		// Not using disk queue, but the next message would go over MaxInMemory
		// Transfer everything to the on-disk queue
		for cq.memQueue.Len() > 0 {
			toEnqueue := cq.memQueue.Remove(cq.memQueue.Front()).(*MessageStruct)
			err := cq.diskQueue.Enqueue(toEnqueue)
			if err != nil {
				log.Errorln("Failed to enqueue message:", err)
			}
		}
		// Enqueue the current
		err := cq.diskQueue.Enqueue(msgStruct)
		if err != nil {
			log.Errorln("Failed to enqueue message:", err)
		}
		cq.usingDisk = true

	} else {
		// Last option is we are using disk
		err := cq.diskQueue.Enqueue(msgStruct)
		if err != nil {
			log.Errorln("Failed to enqueue message:", err)
		}
	}
	cq.emptyCond.Broadcast()
}

// dequeueLocked dequeues a message, assuming the queue has already been locked
func (cq *ConfirmationQueue) dequeueLocked() (*MessageStruct, error) {
	// Check if we have a message available in the queue
	if !cq.usingDisk && cq.memQueue.Len() == 0 {
		return nil, ErrEmpty
	} else if cq.usingDisk && cq.diskQueue.Size() == 0 {
		return nil, ErrEmpty
	}

	if !cq.usingDisk {
		return cq.memQueue.Remove(cq.memQueue.Front()).(*MessageStruct), nil
	} else if cq.usingDisk && (cq.diskQueue.Size()-1) >= LowWaterMark {
		// If we are using disk, and the on disk size is larger than the low water mark
		msgStruct, err := cq.diskQueue.Dequeue()
		if err != nil {
			log.Errorln("Failed to dequeue: ", err)
		}
		return msgStruct.(*MessageStruct), err
	} else {
		// Using disk, but the next enqueue makes it < LowWaterMark, transfer everything from on disk to in-memory
		for cq.diskQueue.Size() > 0 {
			msgStruct, err := cq.diskQueue.Dequeue()
			if err != nil {
				log.Errorln("Failed to dequeue: ", err)
			}
			cq.memQueue.PushBack(msgStruct.(*MessageStruct))
		}
		cq.usingDisk = false
		return cq.memQueue.Remove(cq.memQueue.Front()).(*MessageStruct), nil
	}

}

// Dequeue Blocking function to receive a message
func (cq *ConfirmationQueue) Dequeue() (*MessageStruct, error) {
	cq.mutex.Lock()
	defer cq.mutex.Unlock()
	for {
		msg, err := cq.dequeueLocked()
		if err == ErrEmpty {
			cq.emptyCond.Wait()
			// Wait() atomically unlocks mutexEmptyCond and suspends execution of the calling goroutine.
			// Receiving the signal does not guarantee an item is available, let's loop and check again.
			continue
		} else if err != nil {
			return nil, err
		}
		return msg, nil
	}
}

// Close will close the on-disk files
func (cq *ConfirmationQueue) Close() error {
	cq.mutex.Lock()
	defer cq.mutex.Unlock()
	return cq.diskQueue.Close()
}
