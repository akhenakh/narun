package natsutil

import (
	"fmt"
	"log/slog"
	"slices"
	"strings"
	"time"

	"github.com/nats-io/nats.go"
)

// ConnectNATS establishes a connection to the NATS server.
func ConnectNATS(url string) (*nats.Conn, error) {
	nc, err := nats.Connect(url,
		nats.Name("NATS HTTP Gateway"),
		nats.Timeout(10*time.Second),
		nats.PingInterval(20*time.Second),
		nats.MaxPingsOutstanding(5),
		nats.ReconnectWait(2*time.Second),
		nats.MaxReconnects(-1), // Retry forever
		nats.DisconnectErrHandler(func(nc *nats.Conn, err error) {
			// Use default logger or inject one if needed for consistent logging
			slog.Error(fmt.Sprintf("NATS disconnected: %v. Will attempt reconnect.", err))
		}),
		nats.ReconnectHandler(func(nc *nats.Conn) {
			slog.Info(fmt.Sprintf("NATS reconnected to %s", nc.ConnectedUrl()))
		}),
		nats.ClosedHandler(func(nc *nats.Conn) {
			slog.Info("NATS connection closed.")
		}),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to NATS at %s: %w", url, err)
	}
	return nc, nil
}

// SetupJetStream ensures the required JetStream stream exists.
// It now takes a single stream name and derives the necessary subject pattern.
func SetupJetStream(logger *slog.Logger, nc *nats.Conn, natsStream string) (nats.JetStreamContext, error) {
	if strings.TrimSpace(natsStream) == "" {
		return nil, fmt.Errorf("stream name cannot be empty")
	}
	if strings.ContainsAny(natsStream, ".*> ") {
		return nil, fmt.Errorf("invalid nats_stream name '%s': contains invalid characters", natsStream)
	}

	js, err := nc.JetStream(nats.PublishAsyncMaxPending(256))
	if err != nil {
		return nil, fmt.Errorf("failed to create JetStream context: %w", err)
	}

	logger.Info("Checking/Creating JetStream Stream", "stream", natsStream)

	streamInfo, err := js.StreamInfo(natsStream)
	streamSubject := fmt.Sprintf("%s.*", natsStream) // Expected subject pattern

	// If stream doesn't exist, create it
	if err != nil {
		if err == nats.ErrStreamNotFound {
			logger.Info("Stream not found, creating", "stream", natsStream)
			// Create the stream with the derived subject pattern
			_, err = js.AddStream(&nats.StreamConfig{
				Name:     natsStream,
				Subjects: []string{streamSubject}, // Only need the wildcard subject
				// Default Policies: WorkQueue for consumers, Limits for retention
				Storage:   nats.FileStorage,     // Use FileStorage for persistence
				Retention: nats.WorkQueuePolicy, // Ensures message consumed only once per queue group
				// Consider other options: MaxBytes, MaxMsgs, MaxAge, Replicas etc.
			})
			if err != nil {
				return nil, fmt.Errorf("failed to create stream %s: %w", natsStream, err)
			}
			logger.Info("Stream created", "stream", natsStream, "subject_pattern", streamSubject)
		} else {
			// Other error retrieving stream info
			return nil, fmt.Errorf("failed to get stream info for %s: %w", natsStream, err)
		}
	} else {
		// Stream exists
		logger.Info("Stream already exists", "stream", natsStream) // Avoid logging full config by default

		// Validate existing stream configuration
		if streamInfo.Config.Retention != nats.WorkQueuePolicy {
			logger.Warn("Existing stream retention policy is not WorkQueuePolicy. Consumers might not behave as expected.",
				"stream", natsStream, "retention", streamInfo.Config.Retention)
			// Optionally, update the stream or return an error based on requirements
		}

		// Ensure the existing stream covers the required subject pattern
		if !slices.Contains(streamInfo.Config.Subjects, streamSubject) {
			// Attempt to update the stream to include the subject
			logger.Warn("Existing stream does not cover expected subject pattern, attempting update",
				"stream", natsStream, "expected_subject", streamSubject, "existing_subjects", streamInfo.Config.Subjects)

			updatedConfig := streamInfo.Config
			// Add the subject if it's not already covered by a broader pattern like ">"
			shouldAdd := true
			for _, subj := range updatedConfig.Subjects {
				if subj == ">" || subj == streamSubject { // If already covered, don't add duplicate
					shouldAdd = false
					break
				}
				// More complex wildcard matching could be added here if needed
			}
			if shouldAdd {
				updatedConfig.Subjects = append(updatedConfig.Subjects, streamSubject)
				_, err = js.UpdateStream(&updatedConfig)
				if err != nil {
					logger.Error("Failed to update existing stream with required subject",
						"stream", natsStream, "subject", streamSubject, "error", err)
					return nil, fmt.Errorf("failed to update stream %s with required subject %s: %w", natsStream, streamSubject, err)
				}
				logger.Info("Successfully updated stream with required subject", "stream", natsStream, "subject", streamSubject)
			}
		} else {
			logger.Debug("Existing stream already covers the required subject pattern", "stream", natsStream, "subject", streamSubject)
		}
	}

	return js, nil
}
