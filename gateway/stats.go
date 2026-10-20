package gateway

import (
	"sync"
	"time"
)

// DecisionEvent is one rate-limit verdict, broadcast to the dashboard.
type DecisionEvent struct {
	At         time.Time `json:"at"`
	Key        string    `json:"key"`
	Path       string    `json:"path"`
	Allowed    bool      `json:"allowed"`
	Algorithm  string    `json:"algorithm"`
	Upstream   string    `json:"upstream,omitempty"`
	LatencyMs  float64   `json:"latency_ms"`
	StatusCode int       `json:"status_code"`
}

// Stats aggregates decisions and fans them out to live subscribers.
type Stats struct {
	mu        sync.Mutex
	total     int
	allowed   int
	denied    int
	perKey    map[string]*keyStats
	recent    []DecisionEvent
	subs      map[chan DecisionEvent]struct{}
	startedAt time.Time
}

type keyStats struct {
	Allowed int `json:"allowed"`
	Denied  int `json:"denied"`
}

func NewStats() *Stats {
	return &Stats{
		perKey:    map[string]*keyStats{},
		subs:      map[chan DecisionEvent]struct{}{},
		startedAt: time.Now(),
	}
}

func (s *Stats) Record(ev DecisionEvent) {
	s.mu.Lock()
	s.total++
	if ev.Allowed {
		s.allowed++
	} else {
		s.denied++
	}
	ks := s.perKey[ev.Key]
	if ks == nil {
		ks = &keyStats{}
		s.perKey[ev.Key] = ks
	}
	if ev.Allowed {
		ks.Allowed++
	} else {
		ks.Denied++
	}
	s.recent = append(s.recent, ev)
	if len(s.recent) > 200 {
		s.recent = s.recent[len(s.recent)-200:]
	}
	subs := make([]chan DecisionEvent, 0, len(s.subs))
	for ch := range s.subs {
		subs = append(subs, ch)
	}
	s.mu.Unlock()
	for _, ch := range subs {
		select {
		case ch <- ev:
		default: // slow dashboards drop events rather than block the gateway
		}
	}
}

func (s *Stats) Subscribe() chan DecisionEvent {
	ch := make(chan DecisionEvent, 256)
	s.mu.Lock()
	s.subs[ch] = struct{}{}
	s.mu.Unlock()
	return ch
}

func (s *Stats) Unsubscribe(ch chan DecisionEvent) {
	s.mu.Lock()
	delete(s.subs, ch)
	s.mu.Unlock()
}

type Snapshot struct {
	Total     int                  `json:"total"`
	Allowed   int                  `json:"allowed"`
	Denied    int                  `json:"denied"`
	DenyRate  float64              `json:"deny_rate"`
	PerKey    map[string]*keyStats `json:"per_key"`
	Recent    []DecisionEvent      `json:"recent"`
	UptimeSec float64              `json:"uptime_sec"`
}

func (s *Stats) Snapshot() Snapshot {
	s.mu.Lock()
	defer s.mu.Unlock()
	recent := make([]DecisionEvent, len(s.recent))
	copy(recent, s.recent)
	perKey := map[string]*keyStats{}
	for k, v := range s.perKey {
		c := *v
		perKey[k] = &c
	}
	deny := 0.0
	if s.total > 0 {
		deny = float64(s.denied) / float64(s.total)
	}
	return Snapshot{Total: s.total, Allowed: s.allowed, Denied: s.denied,
		DenyRate: deny, PerKey: perKey, Recent: recent,
		UptimeSec: time.Since(s.startedAt).Seconds()}
}
