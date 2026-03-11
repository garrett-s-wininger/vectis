package backoff

import "time"

func RetryWithBackoff(maxTries int, baseDelay time.Duration, fn func() error, onRetry func(attempt int, nextDelay time.Duration, err error)) error {
	var lastErr error
	for attempt := range maxTries {
		lastErr = fn()
		if lastErr == nil {
			return nil
		}

		if attempt == maxTries-1 {
			break
		}

		delay := baseDelay * time.Duration(1<<attempt)
		if onRetry != nil {
			onRetry(attempt+1, delay, lastErr)
		}

		time.Sleep(delay)
	}

	return lastErr
}
