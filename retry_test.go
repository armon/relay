package relay

import (
	"math/rand"
	"reflect"
	"testing"
	"time"

	"github.com/armon/relay/broker"
	"github.com/hashicorp/go-uuid"
)

func TestRetryBroker_implements(t *testing.T) {
	var _ broker.Broker = &retryBroker{}
}

func TestRetryBroker(t *testing.T) {
	// Make the actual relay layer
	r, err := New(&Config{})
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	// Make a retry broker
	broker := r.RetryBroker(13, 100*time.Millisecond, 500*time.Millisecond)
	if broker.attempts != 13 {
		t.Fatalf("bad attempts: %d", broker.attempts)
	}
	if broker.min != 100*time.Millisecond {
		t.Fatalf("bad min: %s", broker.min)
	}
	if broker.max != 500*time.Millisecond {
		t.Fatalf("bad max: %s", broker.max)
	}
}

func TestRetryBroker_Consumer(t *testing.T) {
	// Make the broker
	r, err := New(&Config{})
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	broker := r.RetryBroker(5, 100*time.Millisecond, 500*time.Millisecond)

	// Make the consumer
	cons, err := broker.Consumer("test")
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	tcons := cons.(*retryConsumer)
	if tcons.attempts != 5 {
		t.Fatalf("bad attempts: %d", tcons.attempts)
	}
	if tcons.min != 100*time.Millisecond {
		t.Fatalf("bad min: %s", tcons.min)
	}
	if tcons.max != 500*time.Millisecond {
		t.Fatalf("bad max: %s", tcons.max)
	}
	if tcons.queue != "test" {
		t.Fatalf("bad queue: %q", tcons.queue)
	}
}

func TestRetryBroker_Publisher(t *testing.T) {
	// Make the broker
	r, err := New(&Config{})
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	broker := r.RetryBroker(5, 100*time.Millisecond, 500*time.Millisecond)

	// Make the publisher
	pub, err := broker.Publisher("test")
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	tpub := pub.(*retryPublisher)
	if tpub.attempts != 5 {
		t.Fatalf("bad attempts: %d", tpub.attempts)
	}
	if tpub.min != 100*time.Millisecond {
		t.Fatalf("bad min: %s", tpub.min)
	}
	if tpub.max != 500*time.Millisecond {
		t.Fatalf("bad max: %s", tpub.max)
	}
	if tpub.queue != "test" {
		t.Fatalf("bad queue: %q", tpub.queue)
	}
}

func TestRetryBrokerInteg(t *testing.T) {
	CheckInteg(t)

	payloads := make([]int, 100)
	for i := 0; i < 100; i++ {
		payloads[i] = i + 1
	}

	// Create the config
	conf := &Config{Addr: AMQPHost()}
	r, err := New(conf)
	if err != nil {
		panic(err)
	}

	// Make a retrying broker
	b := r.RetryBroker(10, 10*time.Millisecond, 10*time.Second)

	// Make a random tests queue name
	queueName, err := uuid.GenerateUUID()
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	// Get the publisher and consumer
	pub, err := b.Publisher(queueName)
	if err != nil {
		panic(err)
	}
	cons, err := b.Consumer(queueName)
	if err != nil {
		panic(err)
	}

	// Periodically close the connection
	go func() {
		for {
			time.Sleep(randomStagger(time.Second))
			if r.pubConn != nil {
				r.pubConn.Close()
			}
			if r.consConn != nil {
				r.consConn.Close()
			}
		}
	}()

	// Set a deadline for the test
	time.AfterFunc(time.Minute, func() { t.Fatalf("timed out") })

	// Pubish all of the messages
	for _, payload := range payloads {
		if err := pub.Publish(payload); err != nil {
			t.Fatalf("err: %v", err)
		}

		// Allow time for the connection to unexpectedly close
		// a couple of times.
		time.Sleep(randomStagger(100 * time.Millisecond))
	}

	// Consume the messages in a loop until we get them all
	var result []int
	seen := make(map[int]struct{}, 100) // Used for deduplication
	for len(result) < 100 {
		var msg int
		if err = cons.ConsumeAck(&msg); err == nil {
			// Check if we have already seen the message. There are multiple
			// ways that messages may become duplicated in the face of network
			// errors. The publisher may have written the message multiple
			// times if publisher confirms failed, and the client may consume
			// the same message multiple times if sending an ack fails.
			if _, ok := seen[msg]; ok {
				continue
			}
			seen[msg] = struct{}{}

			result = append(result, msg)
		}

		// Allow time for the connection to unexpectedly close
		// a couple of times.
		time.Sleep(randomStagger(100 * time.Millisecond))
	}

	// Check that the messages arrived in the same order they were submitted
	if !reflect.DeepEqual(payloads, result) {
		t.Fatalf("\nexpect: %v\nactual: %v", payloads, result)
	}
}

// randomStagger returns a randomized duration +/- 25% of the input.
func randomStagger(interval time.Duration) time.Duration {
	stagger := time.Duration(rand.Int63()) % (interval / 2)
	return 3*(interval/4) + stagger
}

func init() {
	rand.Seed(time.Now().UnixNano())
}
