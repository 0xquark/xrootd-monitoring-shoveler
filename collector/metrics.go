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

	// siteRegistryDomains reports how many domain suffixes the currently loaded
	// CRIC domains map knows about. A value of 0 means every host resolves to
	// unknown_domain (the "domain" resolution method is effectively off).
	siteRegistryDomains = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "shoveler_site_registry_domains",
		Help: "Number of domain suffixes in the currently loaded CRIC domains map",
	})

	// siteRegistryHosts reports how many unique hosts the currently loaded CRIC
	// SE endpoint map knows about. A value of 0 means the "hostname" method is
	// effectively off.
	siteRegistryHosts = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "shoveler_site_registry_hosts",
		Help: "Number of unique hosts in the currently loaded CRIC SE endpoint map",
	})

	// siteRegistryReloadFailures counts background refresh attempts that failed;
	// on failure the previous domains map is retained. A sustained rise indicates
	// the configured CRIC domains URL is unreachable or serving invalid data.
	siteRegistryReloadFailures = promauto.NewCounter(prometheus.CounterOpts{
		Name: "shoveler_site_registry_reload_failures",
		Help: "Total number of CRIC domains map background refresh attempts that failed",
	})

	// siteHostnameReloadFailures counts CRIC SE background refresh attempts that
	// failed; on failure the previous host map is retained.
	siteHostnameReloadFailures = promauto.NewCounter(prometheus.CounterOpts{
		Name: "shoveler_site_hostname_reload_failures",
		Help: "Total number of CRIC SE endpoint map background refresh attempts that failed",
	})

	// siteUnresolved counts transfer endpoints that no configured resolution
	// method could map to an RCSite, broken down by role (src/dst) and reason
	// (ambiguous, unknown_domain, no_host, unknown_ip). This is the primary
	// "how many unknowns" signal; a resolved endpoint is not counted.
	siteUnresolved = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "shoveler_site_unresolved",
		Help: "Total number of transfer endpoints not resolved to an RCSite, by role and reason",
	}, []string{"role", "reason"})

	// siteIPRoutes reports how many CIDR blocks the currently loaded CRIC
	// netroutes table holds. 0 means the "ip" method is effectively off.
	siteIPRoutes = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "shoveler_site_ip_routes",
		Help: "Number of CIDR blocks in the currently loaded CRIC netroutes table",
	})

	// siteIPReloadFailures counts CRIC netroutes background refresh attempts that
	// failed; on failure the previous route table is retained.
	siteIPReloadFailures = promauto.NewCounter(prometheus.CounterOpts{
		Name: "shoveler_site_ip_reload_failures",
		Help: "Total number of CRIC netroutes table background refresh attempts that failed",
	})

	// siteResolvedByIP counts endpoints resolved by CRIC netroutes CIDR
	// containment, by role (src/dst). It quantifies how much the address-based
	// method recovers over name-based matching alone.
	siteResolvedByIP = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "shoveler_site_resolved_by_ip",
		Help: "Total number of transfer endpoints resolved to an RCSite via CRIC netroutes, by role",
	}, []string{"role"})

	// siteResolvedByMethod counts endpoints resolved to an RCSite by role
	// (src/dst) and by which step of site.resolution_order produced the answer
	// (config, hostname, ip, domain). hostname is an exact CRIC SE endpoint match;
	// domain is a suffix match. It shows what each step actually contributes,
	// which is what tuning the order needs.
	siteResolvedByMethod = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "shoveler_site_resolved_by_method",
		Help: "Total number of transfer endpoints resolved to an RCSite, by role and resolution method",
	}, []string{"role", "method"})

	// scitagsRegistryExperiments reports how many experiments the currently loaded
	// SciTags registry knows about. A value of 0 means id->name resolution is off.
	scitagsRegistryExperiments = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "shoveler_scitags_registry_experiments",
		Help: "Number of experiments in the currently loaded SciTags registry",
	})

	// scitagsRegistryActivities reports how many (experiment, activity) pairs the
	// currently loaded SciTags registry knows about.
	scitagsRegistryActivities = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "shoveler_scitags_registry_activities",
		Help: "Number of (experiment, activity) pairs in the currently loaded SciTags registry",
	})

	// scitagsRegistryReloadFailures counts background refresh attempts that failed;
	// on failure the previous registry is retained. A sustained rise indicates the
	// configured SciTags URL is unreachable or serving invalid data.
	scitagsRegistryReloadFailures = promauto.NewCounter(prometheus.CounterOpts{
		Name: "shoveler_scitags_registry_reload_failures_total",
		Help: "Total number of SciTags registry background refresh attempts that failed",
	})

	// scitagsUnmappedIDsTotal counts U-stream experiment/activity ids that could
	// not be resolved to a name against the loaded registry, broken down by which
	// id was missing. A sustained rise usually means the registry snapshot is
	// stale relative to what the servers are tagging.
	scitagsUnmappedIDsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "shoveler_scitags_unmapped_ids_total",
		Help: "Total number of SciTags ids that could not be resolved to a name",
	}, []string{"kind"})
	// packetsPerServerTotal counts successfully parsed monitoring packets broken
	// down by the upstream server IP and the XRootD stream / frame type.  Use
	// this to verify that each XRootD node is still sending and to see which
	// streams (fstat, dict, user, eainfo, map, gstream, …) are active.
	packetsPerServerTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "shoveler_packets_by_server_total",
		Help: "Total number of successfully parsed monitoring packets per upstream server IP and XRootD stream type",
	}, []string{"server_ip", "packet_type"})

	// standaloneCloseRecordsTotal counts file close records for which no matching
	// open was found in stateMap. These records are emitted via createStandaloneCloseRecord
	// and are degraded: they lack LFN, open-time, and full user context. A sustained
	// rise here indicates the correlator is missing opens (e.g. due to packet loss,
	// TTL expiry, or shoveler restart mid-session).
	standaloneCloseRecordsTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "shoveler_standalone_close_records_total",
		Help: "Total number of file close records emitted without a matching open in stateMap (degraded records missing LFN, open-time, and full user context)",
	})

	// fileOpenRecordsTotal counts FileOpen records parsed from file-record packets
	// (both f-stream/fstat and t-stream/trace), broken down by upstream server IP.
	// Each packet can carry multiple records.
	fileOpenRecordsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "shoveler_file_open_records_total",
		Help: "Total number of file open records received in f-stream and t-stream packets per upstream server IP",
	}, []string{"server_ip"})

	// fileCloseRecordsTotal counts FileClose records parsed from file-record packets
	// (both f-stream/fstat and t-stream/trace), broken down by upstream server IP.
	fileCloseRecordsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "shoveler_file_close_records_total",
		Help: "Total number of file close records received in f-stream and t-stream packets per upstream server IP",
	}, []string{"server_ip"})

	// fileTimeRecordsTotal counts FileTime (TOD) records parsed from file-record packets
	// (both f-stream/fstat and t-stream/trace), broken down by upstream server IP.
	fileTimeRecordsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "shoveler_file_time_records_total",
		Help: "Total number of file time (TOD) records received in f-stream and t-stream packets per upstream server IP",
	}, []string{"server_ip"})
)
