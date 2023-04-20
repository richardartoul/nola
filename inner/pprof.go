package inner

import (
	"net/http"
	"net/http/pprof"
)

func AttachPProf(sm *http.ServeMux) {
	sm.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
	sm.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
	sm.HandleFunc("/debug/pprof/trace", pprof.Trace)
	sm.HandleFunc("/debug/pprof/profile", pprof.Profile)
	sm.HandleFunc("/debug/pprof/", pprof.Index)
}
