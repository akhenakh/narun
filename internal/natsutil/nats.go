package natsutil

import (
	"fmt"
	"log/slog"
	"strings"
	"time"

	"slices"

	"github.com/nats-io/nats.go"
)

// ConnectNATS establishes a connection to the NATS server.
// (No changes needed here)
func ConnectNATS(url string) (*nats.Conn, error) {
	nc, err := nats.Connect(url,
		nats.Name("NATS HTTP Gateway"),
		nats.Timeout(10*time.Second),
		nats.PingInterval(20*time.Second),
		nats.MaxPingsOutstanding(5),
		nats.ReconnectWait(2*time.Second),
		nats.MaxReconnects(-1), // Retry forever
		nats.DisconnectErrHandler(func(nc *nats.Conn, err error) {
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
func SetupJetStream(logger *slog.Logger, nc *nats.Conn, natsStream string) (nats.JetStreamContext, error) {
	if strings.TrimSpace(natsStream) == "" {
		return nil, fmt.Errorf("stream name cannot be empty")
	}

	js, err := nc.JetStream(nats.PublishAsyncMaxPending(256))
	if err != nil {
		return nil, fmt.Errorf("failed to create JetStream context: %w", err)
	}

	logger.Info("Checking/Creating JetStream Stream", "stream", natsStream)

	streamInfo, err := js.StreamInfo(natsStream)

	// If stream doesn't exist, create it
	if err == nats.ErrStreamNotFound {
		logger.Info("Stream not found, creating", "stream", natsStream)

		// Derive subjects based on the config prefix and stream name
		// Example: If  natsStream="ORDERS", subject becomes "ORDERS.*"
		streamSubject := fmt.Sprintf("%s.*", natsStream)

		_, err = js.AddStream(&nats.StreamConfig{
			Name:     natsStream,
			Subjects: []string{streamSubject},
			// Consider other options: Storage, Retention, Replicas etc.
			Storage:   nats.FileStorage,     // Use FileStorage for persistence
			Retention: nats.WorkQueuePolicy, // Ensures message consumed only once per queue group
			// Add other configurations as needed: MaxBytes, MaxMsgs, MaxAge, Replicas, etc.
		})
		if err != nil {
			return nil, fmt.Errorf("failed to create stream %s: %w", natsStream, err)
		}
		logger.Info("Stream created", "stream", natsStream, "subject_pattern", streamSubject)
	} else if err != nil {
		// Other error retrieving stream info
		return nil, fmt.Errorf("failed to get stream info for %s: %w", natsStream, err)
	} else {
		// Stream exists
		logger.Info("Stream already exists", "stream", natsStream, "config", streamInfo.Config)
		// Optional: Update stream config if needed using js.UpdateStream()
		// Example check: Ensure the existing stream covers the required subject pattern
		found := false
		streamSubject := fmt.Sprintf("%s.*", natsStream)

		if slices.Contains(streamInfo.Config.Subjects, streamSubject) {
			found = true
		}
		if !found {
			logger.Warn("Existing stream does not explicitly cover expected subject pattern",
				"stream", natsStream, "expected_subject", streamSubject, "existing_subjects", streamInfo.Config.Subjects)
			// Return an error to prevent using a stream without proper subject patterns
			return nil, fmt.Errorf("existing stream %s does not have the required subject pattern %s", natsStream, streamSubject)
		}
	}

	return js, nil
}
