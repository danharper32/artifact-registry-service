package webhooks

import "time"

// defaultBackoff is the delay BEFORE each attempt (index = attempt number, 0-based).
// Attempt 0 fires immediately; subsequent attempts wait the listed duration.
var defaultBackoff = []time.Duration{
	0,
	1 * time.Second,
	5 * time.Second,
	30 * time.Second,
	2 * time.Minute,
}
