package relay

import (
	"errors"
	"log"
	"sync"
	"time"

	"github.com/armon/relay/broker"
)

var (
	// Error returned when an invalid number of attempts is specified. Protects
	// overflows of these values.
	errAttemptsRange = errors.New("Attempts must be between 1 and 32")

	// Error returned if a zero value is given for min/max wait time.
	errWaitRange = errors.New("Min/Max wait times must be positive")
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
func (r *Relay) RetryBroker(attempts int, min, max time.Duration) (*retryBroker, error) {
	if attempts < 1 || attempts > 32 {
		return nil, errAttemptsRange
	}
	if min == 0 || max == 0 {
		return nil, errWaitRange
	}
	return &retryBroker{&relayBroker{r}, attempts, min, max}, nil
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

// discard is used to remove a broken consumer.
func (rc *retryConsumer) discard(cons broker.Consumer) {
	if cons == nil {
		return
	}
	defer cons.Close()

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
	err = cons.Ack()
	if err != nil {
		log.Printf("[ERR] relay: consumer failed ack: %v", err)
		rc.discard(cons)
	}
	return err
}

// Nack sends message(s) back to the queue.
func (rc *retryConsumer) Nack() error {
	cons, err := rc.consumer(false)
	if err != nil || cons == nil {
		return err
	}
	err = cons.Nack()
	if err != nil {
		log.Printf("[ERR] relay: consumer failed nack: %v", err)
		rc.discard(cons)
	}
	return err
}

// ConsumeAck is used to consume with automatic ack. The consume operation
// is able to be retried, but if a message is consumed and the acknowledgement
// fails, the broker will re-deliver the message. Because of this, it is
// critical that the returned error is checked, as it is possible that both
// the output is written and an error is encountered if we see connection
// loss between Consume() and Ack().
func (rc *retryConsumer) ConsumeAck(out interface{}) error {
	if err := rc.Consume(out); err != nil {
		return err
	}
	return rc.Ack()
}

// Consume consumes a single message from the queue.
func (rc *retryConsumer) Consume(out interface{}) error {
	return rc.ConsumeTimeout(out, -1)
}

// ConsumeTimeout consumes a single message from the queue with an upper bound
// on the time spent waiting.
func (rc *retryConsumer) ConsumeTimeout(out interface{}, timeout time.Duration) error {
	// Record the deadline so we can honor the timeout
	var deadline time.Time
	if timeout >= 0 {
		deadline = time.Now().Add(timeout)
	}

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

		// Check if we are already passed the deadline
		now := time.Now()
		if !deadline.IsZero() && now.After(deadline) {
			log.Printf("[DEBUG] relay: consumer reached deadline")
			return TimedOut
		}

		// Calculate the next wait period
		wait := rc.min * time.Duration(int64(1)<<uint(i))
		if wait > rc.max {
			wait = rc.max
		}

		// Ensure we don't wait past the deadline. Adjust the timeout
		// so that the next call to consume will return earlier.
		if !deadline.IsZero() && now.Add(wait).After(deadline) {
			wait = deadline.Sub(now)
		}
		timeout -= wait

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

// discard is used to remove a broken publisher.
func (rp *retryPublisher) discard(pub broker.Publisher) {
	if pub == nil {
		return
	}
	defer pub.Close()

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
// the broker automatically tries to replace the underlying channel and submits
// the message again. If publisher confirms are enabled, this may result in the
// same message being published multiple times. If this is not desirable, the
// config struct allows disabling publisher confirms, which may drop messages.
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
