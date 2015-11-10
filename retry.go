package relay

import (
	"log"
	"strings"
	"time"

	"github.com/armon/relay/broker"
)

// retryBroker implements a resilient AMQP broker which automatically replaces
// the underlying channel if an unexpected error is observed. Due to AMQP's
// persistent connections, this becomes necessary to survive even minor network
// hiccups which may temporarily sever the connection to the server.
//
// The retryBroker does not come without a price, however. Clients are still
// responsible for deduplicating messages or handling them idempotently, as
// duplications are a regular happening in the face of network errors due to
// message redelivery provided by the protocol. It is also important to note
// that acknowledgements - both ack and nack - cannot be retried across
// connection resets. This stems from the fact that a given consumer's channel
// cannot be revived and must be thrown away if the consumer is to be reused.
type retryBroker struct {
	broker   *relayBroker
	attempts int
	min      time.Duration
	max      time.Duration
}

// RetryBroker returns a new retrying broker with the given settings.
func (r *Relay) RetryBroker(attempts int, min, max time.Duration) *retryBroker {
	return &retryBroker{&relayBroker{r}, attempts, min, max}
}

// Publisher returns a new retrying broker.Publisher.
func (rb *retryBroker) Publisher(queue string) (broker.Publisher, error) {
	return &retryPublisher{rb.broker, nil, queue, rb.attempts, rb.min, rb.max}, nil
}

// Consumer returns a new retrying broker.Consumer.
func (rb *retryBroker) Consumer(queue string) (broker.Consumer, error) {
	return &retryConsumer{rb.broker, nil, queue, rb.attempts, rb.min, rb.max}, nil
}

// Close closes the broker.
func (rb *retryBroker) Close() error {
	if rb.broker != nil {
		return rb.broker.Close()
	}
	return nil
}

// retryConsumer tracks the relay broker and an associated consumer, allowing
// the consumer to be replaced transparently in the face of failure.
type retryConsumer struct {
	broker   broker.Broker
	cons     broker.Consumer
	queue    string
	attempts int
	min      time.Duration
	max      time.Duration
}

// connect is used to connect the consumer to the relay queue.
func (rc *retryConsumer) connect() error {
	if rc.cons != nil {
		return nil
	}

	cons, err := rc.broker.Consumer(rc.queue)
	if err == nil {
		rc.cons = cons
	}
	return err
}

// Close closes the consumer.
func (rc *retryConsumer) Close() error {
	if rc.cons != nil {
		return rc.cons.Close()
	}
	return nil
}

// Ack marks message(s) as delivered.
func (rc *retryConsumer) Ack() error {
	if rc.cons != nil {
		return rc.cons.Ack()
	}
	return nil
}

// Nack sends message(s) back to the queue.
func (rc *retryConsumer) Nack() error {
	if rc.cons != nil {
		return rc.cons.Nack()
	}
	return nil
}

// ConsumeAck is used to consume with automatic ack.
func (rc *retryConsumer) ConsumeAck(out interface{}) error {
	if err := rc.Consume(out); err != nil {
		return err
	}
	if err := rc.Ack(); err != nil {
		return err
	}
	return nil
}

// Consume consumes a single message from the queue.
func (rc *retryConsumer) Consume(out interface{}) error {
	return rc.ConsumeTimeout(out, -1)
}

// ConsumeTimeout consumes a single message from the queue with an upper bound
// on the time spent waiting. This behaves slightly different in the retry
// broker, as we may encounter errors getting a valid consumer. In this case,
// the timeout may actually be longer than specified to allow reconnection
// attempts to take place.
func (rc *retryConsumer) ConsumeTimeout(out interface{}, timeout time.Duration) error {
	wait := rc.min
	for i := 0; ; i++ {
		err := rc.connect()
		if err != nil {
			goto RETRY
		}

		err = rc.cons.ConsumeTimeout(out, timeout)
		if err == nil {
			break
		}

		// Respect the case where the consume legitimately times out.
		if err == TimedOut {
			return err
		}

		// Retries won't help decode errors, so just return.
		if strings.Contains(err.Error(), "Failed to decode") {
			return err
		}

	RETRY:
		log.Printf("[ERR] relay: consumer got error: %v", err)
		rc.Close()
		rc.cons = nil
		if i == rc.attempts {
			log.Printf("[ERR] relay: consumer giving up after %d attempts", i)
			return err
		}
		log.Printf("[DEBUG] relay: consumer retrying in %s", wait)
		time.Sleep(wait)
		wait *= 2
		if wait > rc.max {
			wait = rc.max
		}
	}
	return nil
}

// retryPublisher wraps a relay broker and an associated publisher, allowing
// the underlying publisher's channel to be automatically swapped out if any
// errors are seen during a publish.
type retryPublisher struct {
	broker   broker.Broker
	pub      broker.Publisher
	queue    string
	attempts int
	min      time.Duration
	max      time.Duration
}

// connect connects the publisher using the standard broker interface.
func (rp *retryPublisher) connect() error {
	if rp.pub != nil {
		return nil
	}

	pub, err := rp.broker.Publisher(rp.queue)
	if err == nil {
		rp.pub = pub
	}
	return err
}

// Close closes the publisher.
func (rp *retryPublisher) Close() error {
	if rp.pub != nil {
		return rp.pub.Close()
	}
	return nil
}

// Publish publishes a single message to the queue. If an error is encountered,
// the broker automatically tries to replace the underlying channel.
func (rp *retryPublisher) Publish(in interface{}) error {
	wait := rp.min
	for i := 0; ; i++ {
		err := rp.connect()
		if err != nil {
			goto RETRY
		}

		err = rp.pub.Publish(in)
		if err == nil {
			break
		}

	RETRY:
		log.Printf("[ERR] relay: publisher got error: %v", err)
		rp.Close()
		rp.pub = nil
		if i == rp.attempts {
			log.Printf("[ERR] relay: publisher giving up after %d attempts", i)
			return err
		}
		log.Printf("[DEBUG] relay: publisher retrying in %s", wait)
		time.Sleep(wait)
		wait *= 2
		if wait > rp.max {
			wait = rp.max
		}
	}
	return nil
}
