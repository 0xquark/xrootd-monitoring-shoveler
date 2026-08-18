package shoveler

import (
	"os"
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

// TestQueueInsert tests the good validation
func TestQueueInsert(t *testing.T) {
	queuePath := path.Join(t.TempDir(), "shoveler-queue")
	config := Config{QueueDir: queuePath}
	queue := NewConfirmationQueue(&config)
	defer func(queue *ConfirmationQueue) {
		err := queue.Close()
		if err != nil {
			assert.NoError(t, err)
		}
	}(queue)
	queue.Enqueue([]byte("test1"))
	queue.Enqueue([]byte("test2"))
	msg, err := queue.Dequeue()
	assert.NoError(t, err)
	assert.Equal(t, []byte("test1"), msg.Message)

	msg, err = queue.Dequeue()
	assert.NoError(t, err)
	assert.Equal(t, []byte("test2"), msg.Message)

}

// TestQueueEmptyDequeue Make sure the queue stalls on a third dequeue
func TestQueueEmptyDequeue(t *testing.T) {
	queuePath := path.Join(t.TempDir(), "shoveler-queue")
	config := Config{QueueDir: queuePath}
	queue := NewConfirmationQueue(&config)
	queue.Enqueue([]byte("test1"))
	defer func(queue *ConfirmationQueue) {
		err := queue.Close()
		if err != nil {
			assert.NoError(t, err)
		}
	}(queue)
	msg, err := queue.Dequeue()
	assert.NoError(t, err)
	assert.Equal(t, []byte("test1"), msg.Message)
	doneChan := make(chan bool)
	go func() {
		_, err := queue.Dequeue()
		assert.NoError(t, err)
		doneChan <- true
	}()
	select {
	case <-doneChan:
		assert.Fail(t, "Dequeue Returned before expected")
	case <-time.After(100 * time.Millisecond):
	}

	queue.Enqueue([]byte("test1"))
	select {
	case <-doneChan:
	case <-time.After(100 * time.Millisecond):
		assert.Fail(t, "Dequeue did not return as expected")
	}

}

// TestQueueLotsEntries adds many, many entries to the queue, and makes sure they are de-queued correctly
func TestQueueLotsEntries(t *testing.T) {

	queuePath := path.Join(t.TempDir(), "shoveler-queue")
	config := Config{QueueDir: queuePath}
	queue := NewConfirmationQueue(&config)
	defer func(queue *ConfirmationQueue) {
		err := queue.Close()
		if err != nil {
			assert.NoError(t, err)
		}
	}(queue)
	for i := 1; i <= 100000; i++ {
		msgString := "test." + strconv.Itoa(i)
		queue.Enqueue([]byte(msgString))
	}

	//assert.Equal(t, 100000, queue.Size())
	for i := 1; i <= 100000; i++ {
		msgString := "test." + strconv.Itoa(i)
		msg, err := queue.Dequeue()
		assert.NoError(t, err)
		assert.Equal(t, msgString, string(msg.Message))
	}
	assert.Equal(t, 0, queue.Size())
	for i := 1; i <= 100000; i++ {
		msgString := "test." + strconv.Itoa(i)
		queue.Enqueue([]byte(msgString))
	}

	assert.Equal(t, 100000, queue.Size())
	for i := 1; i <= 100000; i++ {
		msgString := "test." + strconv.Itoa(i)
		msg, err := queue.Dequeue()
		assert.NoError(t, err)
		assert.Equal(t, msgString, string(msg.Message))
	}

}

// corruptSegment truncates a queue segment mid-record, reproducing what an
// abrupt process exit (or an external cleaner) leaves behind: a gob stream that
// ends before the value does. dque reports this as "segment file ... is
// corrupted: error reading gob data from file: unexpected EOF".
func corruptSegment(t *testing.T, queuePath string) (string, []byte) {
	t.Helper()

	entries, err := os.ReadDir(queuePath)
	assert.NoError(t, err, "queue directory should be readable")

	for _, entry := range entries {
		if !strings.HasSuffix(entry.Name(), ".dque") {
			continue
		}

		segment := filepath.Join(queuePath, entry.Name())
		contents, err := os.ReadFile(segment)
		assert.NoError(t, err, "segment should be readable")
		assert.NotEmpty(t, contents, "segment should hold at least one message")

		// Drop the final byte so the last gob value is always incomplete. Cutting
		// at an arbitrary fraction of the file could land on a record boundary and
		// leave a perfectly valid stream, which would fail the test for the wrong
		// reason.
		truncated := contents[:len(contents)-1]
		err = os.WriteFile(segment, truncated, 0600)
		assert.NoError(t, err, "segment should be writable")
		return segment, truncated
	}

	t.Fatalf("no .dque segment found in %s", queuePath)
	return "", nil
}

// TestQueueRecoversFromCorruptSegment is the regression test for the collector
// crash-looping on startup: a truncated segment used to panic in Init, so
// systemd restarted the process straight back onto the same file and the
// collector never came up again. The segment must now be moved aside and the
// queue opened without it.
func TestQueueRecoversFromCorruptSegment(t *testing.T) {
	queuePath := path.Join(t.TempDir(), "shoveler-queue")
	config := Config{QueueDir: queuePath}

	// Fill past MaxInMemory so messages are actually written to disk.
	queue := NewConfirmationQueue(&config)
	for i := 0; i < MaxInMemory+10; i++ {
		queue.Enqueue([]byte("msg" + strconv.Itoa(i)))
	}
	assert.NoError(t, queue.Close(), "queue should close cleanly")

	segment, truncated := corruptSegment(t, queuePath)

	// Re-opening must succeed rather than panic.
	reopened := NewConfirmationQueue(&config)
	defer func() {
		assert.NoError(t, reopened.Close(), "reopened queue should close cleanly")
	}()

	// dque recreates a segment under the original name once the bad one is out of
	// the way, so the check is that the corrupt bytes were preserved alongside it.
	matches, err := filepath.Glob(segment + ".corrupt-*")
	assert.NoError(t, err)
	assert.Len(t, matches, 1, "corrupt segment should be preserved, not deleted")

	quarantined, err := os.ReadFile(matches[0])
	assert.NoError(t, err)
	assert.Equal(t, truncated, quarantined, "quarantined file should hold the corrupt bytes verbatim")

	// The queue is usable again.
	reopened.Enqueue([]byte("after-recovery"))
	msg, err := reopened.Dequeue()
	assert.NoError(t, err, "queue should serve messages after recovery")
	assert.NotNil(t, msg)
}

// TestCorruptSegmentPath covers the parsing of dque's error text, including the
// guard that keeps a path from outside the queue directory from being renamed.
func TestCorruptSegmentPath(t *testing.T) {
	queueDir := "/var/spool/xrootd-monitoring-shoveler/queue"

	tests := []struct {
		name     string
		errMsg   string
		expected string
	}{
		{
			name:     "real dque corruption error",
			errMsg:   "unable to create queue segment in " + queueDir + ": unable to load queue segment in " + queueDir + ": segment file " + queueDir + "/0000000000988.dque is corrupted: error reading gob data from file: unexpected EOF",
			expected: queueDir + "/0000000000988.dque",
		},
		{
			name:     "extra whitespace before the corruption marker",
			errMsg:   "segment file " + queueDir + "/0000000000988.dque  is corrupted: error reading gob data from file: unexpected EOF",
			expected: queueDir + "/0000000000988.dque",
		},
		{
			name:     "unrelated error",
			errMsg:   "permission denied",
			expected: "",
		},
		{
			name:     "segment outside the queue directory is ignored",
			errMsg:   "segment file /etc/passwd.dque is corrupted: error reading gob data",
			expected: "",
		},
		{
			name:     "traversal out of the queue directory is ignored",
			errMsg:   "segment file " + queueDir + "/../../evil.dque is corrupted: error reading gob data",
			expected: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.expected, corruptSegmentPath(tt.errMsg, queueDir))
		})
	}
}

// TestOpenQueuePropagatesNonCorruptionErrors checks that failures we cannot fix
// by quarantining (an unwritable location, say) are still reported rather than
// retried in a loop.
func TestOpenQueuePropagatesNonCorruptionErrors(t *testing.T) {
	blocked := path.Join(t.TempDir(), "not-a-directory")
	assert.NoError(t, os.WriteFile(blocked, []byte("x"), 0600))

	queuePath := path.Join(blocked, "shoveler-queue")
	_, err := openQueue(path.Base(queuePath), path.Dir(queuePath), queuePath, 10000)
	assert.Error(t, err, "a queue path that cannot be created should return an error")
}
