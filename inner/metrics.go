package inner

import (
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"golang.org/x/exp/slog"
)

type Metrics struct {
	logger *slog.Logger

	registry  *prometheus.Registry
	gatherers prometheus.Gatherers
}

func NewMetrics(logger *slog.Logger) (m *Metrics, err error) {
	reg := prometheus.NewRegistry()

	if err = reg.Register(collectors.NewProcessCollector(collectors.ProcessCollectorOpts{})); err != nil {
		return
	}

	if err = reg.Register(collectors.NewGoCollector()); err != nil {
		return
	}

	return &Metrics{logger: logger, registry: reg, gatherers: prometheus.Gatherers{reg}}, nil
}

func (m *Metrics) AttachMetrics(sm *http.ServeMux) {
	sm.Handle("/metrics", m.handler())
}

func (m *Metrics) handler() http.Handler {
	return promhttp.HandlerFor(
		m.gatherers[0],
		promhttp.HandlerOpts{
			EnableOpenMetrics: true,
			ErrorLog:          slog.NewLogLogger(m.logger.Handler(), slog.LevelError),
		},
	)
}
