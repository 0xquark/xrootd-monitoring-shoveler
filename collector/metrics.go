package collector

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	// enrichmentQueueDropped counts records dropped because the enrichment queue was full.
	enrichmentQueueDropped = promauto.NewCounter(prometheus.CounterOpts{
		Name: "shoveler_enrichment_queue_dropped",
		Help: "Total number of enrichment records dropped because the bounded queue was at capacity",
	})

	// enrichmentQueueSize tracks the current number of pending records in the enrichment queue.
	enrichmentQueueSize = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "shoveler_enrichment_queue_size",
		Help: "Current number of pending records in the enrichment queue",
	})

	// packetsPerServerTotal counts successfully parsed monitoring packets broken
	// down by the upstream server IP and the XRootD stream / frame type.  Use
	// this to verify that each XRootD node is still sending and to see which
	// streams (fstat, dict, user, eainfo, map, gstream, …) are active.
	packetsPerServerTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "shoveler_packets_by_server_total",
		Help: "Total number of successfully parsed monitoring packets per upstream server IP and XRootD stream type",
	}, []string{"server_ip", "packet_type"})
)
