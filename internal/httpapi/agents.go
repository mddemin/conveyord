package httpapi

import "net/http"

// listAgents answers "is my agent connected, and what does it say it can run?"
// Before this existed the only way to know was to read the daemon's log, which
// made a job sitting pending indistinguishable from a job nothing can execute.
//
// It reports the registry as it is right now — connected streams, not a
// history — so an agent that has just died may appear for as long as its
// stream takes to unwind.
func (a *API) listAgents(w http.ResponseWriter, r *http.Request) {
	agents := a.agents.Agents()
	if agents == nil {
		agents = []AgentInfo{} // an empty fleet is [], never null
	}
	if err := writeJSON(w, http.StatusOK, agents); err != nil {
		a.logger(r.Context()).Error("writing response", "err", err, "method", r.Method, "path", r.URL.Path)
	}
}
