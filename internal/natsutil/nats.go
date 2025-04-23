package natsutil

import (
	"fmt"
	"log"
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
			log.Printf("NATS disconnected: %v. Will attempt reconnect.", err)
		}),
		nats.ReconnectHandler(func(nc *nats.Conn) {
			log.Printf("NATS reconnected to %s", nc.ConnectedUrl())
		}),
		nats.ClosedHandler(func(nc *nats.Conn) {
			log.Printf("NATS connection closed.")
		}),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to NATS at %s: %w", url, err)
	}
	log.Printf("Connected to NATS at %s", nc.ConnectedUrl())
	return nc, nil
}

// SetupJetStream ensures the required JetStream streams exist.
func SetupJetStream(nc *nats.Conn, streamNames []string) (nats.JetStreamContext, error) {
	js, err := nc.JetStream(nats.PublishAsyncMaxPending(256))
	if err != nil {
		return nil, fmt.Errorf("failed to create JetStream context: %w", err)
	}

	log.Printf("Checking/Creating JetStream Streams: %v", streamNames)
	for _, streamName := range streamNames {
		streamInfo, err := js.StreamInfo(streamName)
		// If stream doesn't exist, create it
		if err == nats.ErrStreamNotFound {
			log.Printf("Stream %s not found, creating...", streamName)
			_, err = js.AddStream(&nats.StreamConfig{
				Name:     streamName,
				Subjects: []string{fmt.Sprintf("tasks.%s.*", streamName)}, // Example: tasks.ORDERS.*
				// Consider other options: Storage, Retention, Replicas etc.
				Storage:   nats.FileStorage,     // Use FileStorage for persistence
				Retention: nats.WorkQueuePolicy, // Ensures message consumed only once
			})
			if err != nil {
				return nil, fmt.Errorf("failed to create stream %s: %w", streamName, err)
			}
			log.Printf("Stream %s created.", streamName)
		} else if err != nil {
			// Other error retrieving stream info
			return nil, fmt.Errorf("failed to get stream info for %s: %w", streamName, err)
		} else {
			// Stream exists
			log.Printf("Stream %s already exists. Config: %+v", streamName, streamInfo.Config)
			// Optional: Update stream config if needed using js.UpdateStream()
		}
	}

	return js, nil
}
