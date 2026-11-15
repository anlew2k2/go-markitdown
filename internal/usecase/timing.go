package usecase

import "time"

// PhaseObserver receives timing for an internal sub-step (e.g. "download",
// "convert", "split_upload") of a usecase operation, so a caller that wants
// structured timing (the pipeline workers) can observe it without this
// package depending on any particular logging mechanism. A nil observer
// means "don't observe" and must always be safe to call through observe().
type PhaseObserver func(phase string, dur time.Duration, err error)

// observe calls f (if non-nil) with the elapsed time since start and err.
func observe(f PhaseObserver, phase string, start time.Time, err error) {
	if f == nil {
		return
	}
	f(phase, time.Since(start), err)
}
