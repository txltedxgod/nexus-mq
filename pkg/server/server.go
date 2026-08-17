package server

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/txltedxgod/nexus-mq/pkg/broker"
	"github.com/txltedxgod/nexus-mq/pkg/consumer"
	"github.com/txltedxgod/nexus-mq/pkg/proto"
)

// HTTPServer provides REST API, WebSocket streams, and static dashboard.
type HTTPServer struct {
	addr    string
	broker  *broker.Broker
	groups  map[string]*consumer.Group
	groupMu sync.RWMutex
	server  *http.Server
}

// NewHTTPServer initializes an HTTP gateway.
func NewHTTPServer(addr string, b *broker.Broker) *HTTPServer {
	s := &HTTPServer{
		addr:   addr,
		broker: b,
		groups: make(map[string]*consumer.Group),
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/api/v1/produce", s.handleProduce)
	mux.HandleFunc("/api/v1/consume", s.handleConsume)
	mux.HandleFunc("/api/v1/topics", s.handleTopics)
	mux.HandleFunc("/api/v1/groups", s.handleGroups)
	mux.HandleFunc("/metrics", s.handleMetrics)
	mux.HandleFunc("/api/v1/stats", s.handleStats)
	mux.HandleFunc("/health", s.handleHealth)
	mux.HandleFunc("/", s.handleDashboard)

	s.server = &http.Server{
		Addr:         addr,
		Handler:      s.corsMiddleware(mux),
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 10 * time.Second,
	}

	return s
}

func (s *HTTPServer) corsMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Authorization")
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusOK)
			return
		}
		next.ServeHTTP(w, r)
	})
}

func (s *HTTPServer) Start() error {
	return s.server.ListenAndServe()
}

func (s *HTTPServer) Close() error {
	return s.server.Close()
}

func (s *HTTPServer) handleProduce(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req proto.ProduceRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, fmt.Sprintf("invalid JSON payload: %v", err), http.StatusBadRequest)
		return
	}

	if req.Topic == "" || req.Value == "" {
		http.Error(w, "topic and value are required", http.StatusBadRequest)
		return
	}

	resp, err := s.broker.Publish(&req)
	if err != nil {
		http.Error(w, fmt.Sprintf("publish error: %v", err), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(resp)
}

func (s *HTTPServer) handleConsume(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet && r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	topic := r.URL.Query().Get("topic")
	partStr := r.URL.Query().Get("partition")
	offsetStr := r.URL.Query().Get("offset")
	limitStr := r.URL.Query().Get("limit")

	partID, _ := strconv.Atoi(partStr)
	offset, _ := strconv.ParseUint(offsetStr, 10, 64)
	limit, _ := strconv.Atoi(limitStr)
	if limit <= 0 {
		limit = 50
	}

	resp, err := s.broker.Consume(&proto.ConsumeRequest{
		Topic:     topic,
		Partition: partID,
		Offset:    offset,
		Limit:     limit,
	})

	if err != nil {
		http.Error(w, fmt.Sprintf("consume error: %v", err), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(resp)
}

func (s *HTTPServer) handleTopics(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodPost {
		var req struct {
			Name       string `json:"name"`
			Partitions int    `json:"partitions"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		_, err := s.broker.CreateTopic(req.Name, req.Partitions)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusCreated)
		_ = json.NewEncoder(w).Encode(map[string]string{"status": "created", "topic": req.Name})
		return
	}

	topics := s.broker.ListTopics()
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(topics)
}

func (s *HTTPServer) handleGroups(w http.ResponseWriter, r *http.Request) {
	s.groupMu.RLock()
	defer s.groupMu.RUnlock()

	res := make([]map[string]interface{}, 0, len(s.groups))
	for _, g := range s.groups {
		res = append(res, g.Status())
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(res)
}

func (s *HTTPServer) handleMetrics(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/plain; version=0.0.4")
	_, _ = w.Write([]byte(s.broker.Metrics().PrometheusFormat()))
}

func (s *HTTPServer) handleStats(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(s.broker.Metrics().Snapshot())
}

func (s *HTTPServer) handleHealth(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]string{"status": "healthy", "service": "nexus-mq"})
}

func (s *HTTPServer) handleDashboard(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/" {
		http.NotFound(w, r)
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	_, _ = w.Write([]byte(dashboardHTML))
}

const dashboardHTML = `<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>NexusMQ — Cluster Monitor</title>
    <style>
        :root {
            --bg: #090d16;
            --surface: #121826;
            --border: #1f293d;
            --accent: #3b82f6;
            --accent-glow: rgba(59, 130, 246, 0.2);
            --text: #f8fafc;
            --text-dim: #94a3b8;
            --green: #10b981;
            --purple: #8b5cf6;
        }
        * { box-sizing: border-box; margin: 0; padding: 0; font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; }
        body { background: var(--bg); color: var(--text); padding: 24px; min-height: 100vh; }
        header { display: flex; justify-content: space-between; align-items: center; border-bottom: 1px solid var(--border); padding-bottom: 16px; margin-bottom: 24px; }
        .logo { display: flex; align-items: center; gap: 12px; font-size: 22px; font-weight: 700; background: linear-gradient(135deg, #60a5fa, #a78bfa); -webkit-background-clip: text; -webkit-text-fill-color: transparent; }
        .badge { background: rgba(16, 185, 129, 0.15); color: var(--green); border: 1px solid rgba(16, 185, 129, 0.3); padding: 4px 10px; border-radius: 999px; font-size: 12px; font-weight: 600; }
        .grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(220px, 1fr)); gap: 16px; margin-bottom: 24px; }
        .card { background: var(--surface); border: 1px solid var(--border); border-radius: 12px; padding: 20px; box-shadow: 0 4px 20px rgba(0,0,0,0.3); }
        .card h3 { font-size: 13px; color: var(--text-dim); text-transform: uppercase; letter-spacing: 0.05em; margin-bottom: 8px; }
        .card .val { font-size: 28px; font-weight: 700; color: var(--text); }
        .section { background: var(--surface); border: 1px solid var(--border); border-radius: 12px; padding: 20px; margin-bottom: 24px; }
        .section h2 { font-size: 18px; margin-bottom: 16px; display: flex; justify-content: space-between; align-items: center; }
        table { width: 100%; border-collapse: collapse; text-align: left; }
        th, td { padding: 12px 14px; border-bottom: 1px solid var(--border); font-size: 14px; }
        th { color: var(--text-dim); font-weight: 600; }
        .tag { background: #1e293b; padding: 2px 8px; border-radius: 4px; font-size: 12px; font-family: monospace; }
        button { background: var(--accent); color: #fff; border: none; padding: 8px 16px; border-radius: 6px; font-weight: 600; cursor: pointer; transition: 0.2s; }
        button:hover { background: #2563eb; }
    </style>
</head>
<body>
    <header>
        <div class="logo">⚡ NexusMQ <span style="font-size: 14px; color: var(--text-dim); font-weight: 400;">v1.0.0</span></div>
        <div class="badge">● CLUSTER HEALTHY</div>
    </header>

    <div class="grid">
        <div class="card">
            <h3>Messages Produced</h3>
            <div class="val" id="stat-prod">0</div>
        </div>
        <div class="card">
            <h3>Messages Consumed</h3>
            <div class="val" id="stat-cons">0</div>
        </div>
        <div class="card">
            <h3>Bytes Written</h3>
            <div class="val" id="stat-bytes">0 KB</div>
        </div>
        <div class="card">
            <h3>Broker Uptime</h3>
            <div class="val" id="stat-uptime">0s</div>
        </div>
    </div>

    <div class="section">
        <h2>Active Topics & Partitions <button onclick="refreshData()">Refresh</button></h2>
        <table id="topics-table">
            <thead>
                <tr>
                    <th>Topic</th>
                    <th>Partitions</th>
                    <th>Head Offset</th>
                    <th>Segments</th>
                    <th>Action</th>
                </tr>
            </thead>
            <tbody id="topics-body">
                <tr><td colspan="5" style="text-align:center; color: var(--text-dim)">Loading topic data...</td></tr>
            </tbody>
        </table>
    </div>

    <script>
        async function refreshData() {
            try {
                const stats = await (await fetch('/api/v1/stats')).json();
                document.getElementById('stat-prod').innerText = stats.messages_produced.toLocaleString();
                document.getElementById('stat-cons').innerText = stats.messages_consumed.toLocaleString();
                document.getElementById('stat-bytes').innerText = (stats.bytes_written / 1024).toFixed(1) + ' KB';
                document.getElementById('stat-uptime').innerText = Math.floor(stats.uptime_seconds) + 's';

                const topics = await (await fetch('/api/v1/topics')).json();
                const body = document.getElementById('topics-body');
                if (topics.length === 0) {
                    body.innerHTML = '<tr><td colspan="5" style="text-align:center; color: var(--text-dim)">No topics created yet. Publish a message to create automatically!</td></tr>';
                    return;
                }
                body.innerHTML = topics.map(t => {
                    const totalOffset = t.partitions.reduce((acc, p) => acc + p.latest_offset, 0);
                    const totalSegs = t.partitions.reduce((acc, p) => acc + p.segments, 0);
                    return `<tr>
                        <td><strong>${t.topic}</strong></td>
                        <td><span class="tag">${t.partitions.length} partitions</span></td>
                        <td>${totalOffset}</td>
                        <td>${totalSegs}</td>
                        <td><span class="tag">ACTIVE</span></td>
                    </tr>`;
                }).join('');
            } catch (e) {
                console.error(e);
            }
        }
        setInterval(refreshData, 3000);
        refreshData();
    </script>
</body>
</html>`
