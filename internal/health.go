package internal

import (
	"fmt"
	"log"
	"net/http"
	"sync"
)

// HealthChecker tracks the health of the application
type HealthChecker struct {
	mu sync.Mutex
	// errors indicates whether a rule name has an error
	errors map[string]error
}

// Report marks or unmarks a rule name as having an error
func (h *HealthChecker) Report(rule string, err error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.errors == nil {
		h.errors = make(map[string]error)
	}
	if err != nil {
		h.errors[rule] = err
	} else {
		delete(h.errors, rule)
	}
}

func (h *HealthChecker) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	h.mu.Lock()
	var failedRules []string
	for ruleName := range h.errors {
		failedRules = append(failedRules, ruleName)
	}
	h.mu.Unlock()

	if len(failedRules) > 0 {
		w.WriteHeader(http.StatusInternalServerError)
		for _, ruleName := range failedRules {
			if _, err := fmt.Fprintf(w, "Error in rule '%s'\n", ruleName); err != nil {
				log.Printf("Failed to write response: %v", err)
			}
		}
		return
	}
	w.WriteHeader(http.StatusOK)
	if _, err := w.Write([]byte("ok")); err != nil {
		log.Printf("Failed to write response: %v", err)
	}
}
