package relay

import (
	"log"
	"sync"
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
	return &retryPublisher{
		broker:   rb.broker,
		queue:    queue,
		attempts: rb.attempts,
		min:      rb.min,
		max:      rb.max,
	}, nil
}

// Consumer returns a new retrying broker.Consumer.
func (rb *retryBroker) Consumer(queue string) (broker.Consumer, error) {
	return &retryConsumer{
		broker:   rb.broker,
		queue:    queue,
		attempts: rb.attempts,
		min:      rb.min,
		max:      rb.max,
	}, nil
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
	l        sync.RWMutex
}

// consumer is used to connect the consumer to the relay queue.
func (rc *retryConsumer) consumer(create bool) (broker.Consumer, error) {
	// Check for an existing consumer
	rc.l.RLock()
	cons := rc.cons
	rc.l.RUnlock()

	if cons != nil || !create {
		return cons, nil
	}

	// Make a new consumer
	rc.l.Lock()
	defer rc.l.Unlock()

	cons, err := rc.broker.Consumer(rc.queue)
	if err != nil {
		return nil, err
	}
	rc.cons = cons
	return cons, nil
}

// discard is used to remove a known-bad consumer.
func (rc *retryConsumer) discard(cons broker.Consumer) {
	if cons == nil {
		return
	}
	cons.Close()

	rc.l.Lock()
	defer rc.l.Unlock()
	if cons == rc.cons {
		rc.cons = nil
	}
}

// Close closes the consumer.
func (rc *retryConsumer) Close() error {
	cons, err := rc.consumer(false)
	if err != nil || cons == nil {
		return err
	}
	return cons.Close()
}

// Ack marks message(s) as delivered.
func (rc *retryConsumer) Ack() error {
	cons, err := rc.consumer(false)
	if err != nil || cons == nil {
		return err
	}
	return cons.Ack()
}

// Nack sends message(s) back to the queue.
func (rc *retryConsumer) Nack() error {
	cons, err := rc.consumer(false)
	if err != nil || cons == nil {
		return err
	}
	return cons.Nack()
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
	for i := 0; ; i++ {
		cons, err := rc.consumer(true)
		if err != nil {
			goto RETRY
		}

		err = cons.ConsumeTimeout(out, timeout)
		if err == nil {
			break
		}

		// Respect the case where the consume legitimately times out.
		if err == TimedOut {
			return err
		}

	RETRY:
		log.Printf("[ERR] relay: consumer got error: %v", err)
		rc.discard(cons)
		if i == rc.attempts {
			log.Printf("[ERR] relay: consumer giving up after %d attempts", i)
			return err
		}
		wait := rc.min * (1 << uint(i))
		if wait > rc.max {
			wait = rc.max
		}
		log.Printf("[DEBUG] relay: consumer retrying in %s", wait)
		time.Sleep(wait)
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
	l        sync.RWMutex
}

// publisher is used to connect the publisher to the relay queue.
func (rp *retryPublisher) publisher(create bool) (broker.Publisher, error) {
	// Check for an existing publisher
	rp.l.RLock()
	pub := rp.pub
	rp.l.RUnlock()

	if pub != nil || !create {
		return pub, nil
	}

	// Make a new publisher
	rp.l.Lock()
	defer rp.l.Unlock()

	pub, err := rp.broker.Publisher(rp.queue)
	if err != nil {
		return nil, err
	}
	rp.pub = pub
	return pub, nil
}

// discard is used to remove a known-bad publisher.
func (rp *retryPublisher) discard(pub broker.Publisher) {
	if pub == nil {
		return
	}
	pub.Close()

	rp.l.Lock()
	defer rp.l.Unlock()
	if pub == rp.pub {
		rp.pub = nil
	}
}

// Close closes the publisher.
func (rp *retryPublisher) Close() error {
	pub, err := rp.publisher(false)
	if err != nil || pub == nil {
		return err
	}
	return pub.Close()
}

// Publish publishes a single message to the queue. If an error is encountered,
// the broker automatically tries to replace the underlying channel.
func (rp *retryPublisher) Publish(in interface{}) error {
	for i := 0; ; i++ {
		pub, err := rp.publisher(true)
		if err != nil {
			goto RETRY
		}

		err = pub.Publish(in)
		if err == nil {
			break
		}

	RETRY:
		log.Printf("[ERR] relay: publisher got error: %v", err)
		rp.discard(pub)
		if i == rp.attempts {
			log.Printf("[ERR] relay: publisher giving up after %d attempts", i)
			return err
		}
		wait := rp.min * (1 << uint(i))
		if wait > rp.max {
			wait = rp.max
		}
		log.Printf("[DEBUG] relay: publisher retrying in %s", wait)
		time.Sleep(wait)
	}
	return nil
}
