package gateway

import (
	"encoding/json"
	"net/http"
)

// Admin serves the dashboard API: stats snapshot, SSE decision stream,
// and upstream management.
type Admin struct {
	stats *Stats
	gw    *Gateway
	load  *LoadTest
	// AdminToken guards mutating endpoints (POST /api/upstreams,
	// /api/loadtest). Empty keeps them open for local demo use - set it
	// via -admin-token anywhere the dashboard port is reachable by others.
	AdminToken string
}

func NewAdmin(stats *Stats, gw *Gateway) *Admin { return &Admin{stats: stats, gw: gw} }

func (a *Admin) WithLoadTest(lt *LoadTest) *Admin { a.load = lt; return a }

func (a *Admin) Mux() *http.ServeMux {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/stats", a.handleStats)
	mux.HandleFunc("GET /api/stream", a.handleStream)
	mux.HandleFunc("GET /api/upstreams", a.handleUpstreams)
	mux.HandleFunc("POST /api/upstreams", a.handleAddUpstream)
	if a.load != nil {
		mux.HandleFunc("POST /api/loadtest", a.load.HandleStart)
	}
	return mux
}

func (a *Admin) handleStats(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, a.stats.Snapshot())
}

func (a *Admin) handleUpstreams(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, map[string]any{"upstreams": a.gw.cfg.Ring.Members()})
}

func (a *Admin) handleAddUpstream(w http.ResponseWriter, r *http.Request) {
	if a.AdminToken != "" && r.Header.Get("X-Admin-Token") != a.AdminToken {
		http.Error(w, `{"error":"unauthorized"}`, http.StatusUnauthorized)
		return
	}
	var body struct {
		URL string `json:"url"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil || body.URL == "" {
		http.Error(w, `{"error":"need {\"url\": ...}"}`, http.StatusBadRequest)
		return
	}
	a.gw.cfg.Ring.Add(body.URL)
	writeJSON(w, map[string]any{"upstreams": a.gw.cfg.Ring.Members()})
}

func (a *Admin) handleStream(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming unsupported", http.StatusInternalServerError)
		return
	}
	ch := a.stats.Subscribe()
	defer a.stats.Unsubscribe(ch)
	_, _ = w.Write([]byte("event: hello\ndata: {}\n\n"))
	flusher.Flush()
	for {
		select {
		case <-r.Context().Done():
			return
		case ev := <-ch:
			data, _ := json.Marshal(ev)
			_, _ = w.Write([]byte("data: " + string(data) + "\n\n"))
			flusher.Flush()
		}
	}
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(v)
}
