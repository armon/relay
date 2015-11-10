package relay

import (
	"testing"
	"time"

	"github.com/armon/relay/broker"
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
