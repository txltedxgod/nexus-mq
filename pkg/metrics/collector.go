package metrics

import (
	"fmt"
	"sync/atomic"
	"time"
)

// Metrics records real-time broker statistics.
type Metrics struct {
	totalProduced   uint64
	totalConsumed   uint64
	bytesWritten    uint64
	bytesRead       uint64
	activeClients   int64
	startTime       time.Time
}

// NewMetrics initializes collector.
func NewMetrics() *Metrics {
	return &Metrics{
		startTime: time.Now(),
	}
}

func (m *Metrics) IncProduced(bytes uint64) {
	atomic.AddUint64(&m.totalProduced, 1)
	atomic.AddUint64(&m.bytesWritten, bytes)
}

func (m *Metrics) IncConsumed(count uint64, bytes uint64) {
	atomic.AddUint64(&m.totalConsumed, count)
	atomic.AddUint64(&m.bytesRead, bytes)
}

func (m *Metrics) IncClients() {
	atomic.AddInt64(&m.activeClients, 1)
}

func (m *Metrics) DecClients() {
	atomic.AddInt64(&m.activeClients, -1)
}

func (m *Metrics) GetTotalProduced() uint64 {
	return atomic.LoadUint64(&m.totalProduced)
}

func (m *Metrics) GetTotalConsumed() uint64 {
	return atomic.LoadUint64(&m.totalConsumed)
}

// PrometheusFormat exports metrics in standard Prometheus text format.
func (m *Metrics) PrometheusFormat() string {
	uptimeSeconds := time.Since(m.startTime).Seconds()
	return fmt.Sprintf(`# HELP nexus_messages_produced_total Total messages published to broker
# TYPE nexus_messages_produced_total counter
nexus_messages_produced_total %d

# HELP nexus_messages_consumed_total Total messages consumed from broker
# TYPE nexus_messages_consumed_total counter
nexus_messages_consumed_total %d

# HELP nexus_bytes_written_total Total payload bytes written
# TYPE nexus_bytes_written_total counter
nexus_bytes_written_total %d

# HELP nexus_bytes_read_total Total payload bytes read
# TYPE nexus_bytes_read_total counter
nexus_bytes_read_total %d

# HELP nexus_active_clients Current active client connections
# TYPE nexus_active_clients gauge
nexus_active_clients %d

# HELP nexus_uptime_seconds Broker uptime in seconds
# TYPE nexus_uptime_seconds gauge
nexus_uptime_seconds %.2f
`,
		atomic.LoadUint64(&m.totalProduced),
		atomic.LoadUint64(&m.totalConsumed),
		atomic.LoadUint64(&m.bytesWritten),
		atomic.LoadUint64(&m.bytesRead),
		atomic.LoadInt64(&m.activeClients),
		uptimeSeconds,
	)
}

// Snapshot returns map for JSON telemetry endpoints.
func (m *Metrics) Snapshot() map[string]interface{} {
	return map[string]interface{}{
		"messages_produced": atomic.LoadUint64(&m.totalProduced),
		"messages_consumed": atomic.LoadUint64(&m.totalConsumed),
		"bytes_written":     atomic.LoadUint64(&m.bytesWritten),
		"bytes_read":        atomic.LoadUint64(&m.bytesRead),
		"active_clients":    atomic.LoadInt64(&m.activeClients),
		"uptime_seconds":    time.Since(m.startTime).Seconds(),
	}
}
