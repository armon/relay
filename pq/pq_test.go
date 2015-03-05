package pq

import (
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/armon/relay/inmem"
)

func TestPriorityQueue(t *testing.T) {
	b := inmem.NewInmemBroker()

	q, err := NewPriorityQueue(b, 10, "testing", MinQuietPeriod)
	if err != nil {
		t.Fatalf("err: %s", err)
	}
	defer q.Close()

	if q.Max() != 9 {
		t.Fatalf("bad: %d", q.Max())
	}

	if q.Min() != 0 {
		t.Fatalf("bad: %d", q.Min())
	}

	if _, err := NewPriorityQueue(b, 0, "testing", MinQuietPeriod); err == nil {
		t.Fatalf("should have errored")
	}

	if _, err := NewPriorityQueue(nil, 1, "testing", MinQuietPeriod); err == nil {
		t.Fatalf("should have errored")
	}
}

func TestPriorityQueuePublishConsume(t *testing.T) {
	b := inmem.NewInmemBroker()

	q, err := NewPriorityQueue(b, 10, "testing", MinQuietPeriod)
	if err != nil {
		t.Fatalf("err: %s", err)
	}
	defer q.Close()

	if err := q.Publish("hello", 5); err != nil {
		t.Fatalf("err: %s", err)
	}

	var resp string
	cons, pri, err := q.Consume(&resp)
	if err != nil {
		t.Fatalf("err: %s", err)
	}
	defer cons.Close()

	if resp != "hello" {
		t.Fatalf("bad: %#v", resp)
	}

	if pri != 5 {
		t.Fatalf("bad: %d", pri)
	}

	// Publishing at out-of-range priorities should error
	if err := q.Publish("hello", q.Min()-1); err == nil {
		t.Fatalf("should have errored")
	}
	if err := q.Publish("hello", q.Max()+1); err == nil {
		t.Fatalf("should have errored")
	}
}

func TestPriorityQueueConsumer(t *testing.T) {
	b := inmem.NewInmemBroker()

	q, err := NewPriorityQueue(b, 1, "testing", MinQuietPeriod)
	if err != nil {
		t.Fatalf("err: %s", err)
	}

	// Should fail if priority is out of range
	if _, err := q.consumer(q.Min() - 1); err == nil {
		t.Fatalf("should have errored")
	}
	if _, err := q.consumer(q.Max() + 1); err == nil {
		t.Fatalf("should have errored")
	}

	// Should succeed with a valid priority level
	cons, err := q.consumer(q.Max())
	if err != nil {
		t.Fatalf("err: %s", err)
	}
	defer cons.Close()
}

func TestPriorityQueuePublisher(t *testing.T) {
	b := inmem.NewInmemBroker()

	q, err := NewPriorityQueue(b, 10, "testing", MinQuietPeriod)
	if err != nil {
		t.Fatalf("err: %s", err)
	}
	defer q.Close()

	p0, err := q.publisher(0)
	if err != nil {
		t.Fatalf("err: %s", err)
	}
	if !reflect.DeepEqual(p0, q.publishers[0]) {
		t.Fatalf("bad: %#v", p0)
	}

	p9, err := q.publisher(9)
	if err != nil {
		t.Fatalf("err: %s", err)
	}
	if !reflect.DeepEqual(p9, q.publishers[9]) {
		t.Fatalf("bad: %#v", p9)
	}

	if _, err := q.publisher(10); err == nil {
		t.Fatalf("should have errored")
	}
}

func TestPriorityQueueShutdown(t *testing.T) {
	b := inmem.NewInmemBroker()

	q, err := NewPriorityQueue(b, 1, "testing", MinQuietPeriod)
	if err != nil {
		t.Fatalf("err: %s", err)
	}
	defer q.Close()

	if err := q.Publish("hello", 0); err != nil {
		t.Fatalf("err: %s", err)
	}

	var msg string
	if _, _, err := q.Consume(&msg); err != nil {
		t.Fatalf("err: %s", err)
	}

	if q.publishers[0] == nil {
		t.Fatalf("bad: %#v", q.publishers[0])
	}

	if err := q.Close(); err != nil {
		t.Fatalf("err: %s", err)
	}

	if q.publishers[0] != nil {
		t.Fatalf("bad: %#v", q.publishers[0])
	}
}

func TestPriorityQueueConsume(t *testing.T) {
	b := inmem.NewInmemBroker()

	q, err := NewPriorityQueue(b, 10, "testing", MinQuietPeriod)
	if err != nil {
		t.Fatalf("err: %s", err)
	}
	defer q.Close()

	// Publish a few messages at different priorities
	for _, pri := range []int{6, 7, 1} {
		if err := q.Publish(fmt.Sprintf("hello%d", pri), pri); err != nil {
			t.Fatalf("err: %s", err)
		}
	}

	// Consume the messages one by one, we should get them back
	// in the correct priority order
	var msg string
	var msgs []string
	for i := 0; i < 3; i++ {
		cons, _, err := q.Consume(&msg)
		if err != nil {
			t.Fatalf("err: %s", err)
		}
		cons.Ack()
		cons.Close()
		msgs = append(msgs, msg)
	}

	if len(msgs) != 3 {
		t.Fatalf("bad: %#v", msgs)
	}

	expected := []string{"hello7", "hello6", "hello1"}
	if !reflect.DeepEqual(expected, msgs) {
		t.Fatalf("bad: %#v", msgs)
	}
}

func TestPriorityQueueQuietPeriod(t *testing.T) {
	b := inmem.NewInmemBroker()

	q, err := NewPriorityQueue(b, 2, "testing", 600*time.Millisecond)
	if err != nil {
		t.Fatalf("err: %s", err)
	}

	// Send the first message
	if err := q.Publish("hello0", 0); err != nil {
		t.Fatalf("err: %s", err)
	}

	// Delay a second message with higher priority for a little bit
	go func(t *testing.T) {
		time.Sleep(400 * time.Millisecond)
		if err := q.Publish("hello1", 1); err != nil {
			t.Fatalf("err: %s", err)
		}
	}(t)

	// Begin consuming with a quiet period longer than the delay.
	var msg string
	cons, pri, err := q.Consume(&msg)
	if err != nil {
		t.Fatalf("err: %s", err)
	}
	cons.Ack()
	cons.Close()

	// The message consumed should have been the higher priority message which
	// arrived after we began consuming messages from the queue.
	if pri != 1 || msg != "hello1" {
		t.Fatalf("bad: %d: %s", pri, msg)
	}

	// Consume again. The first message we published should have been requeued,
	// so here we will attempt to read again with no higher priority messages.
	cons, pri, err = q.Consume(&msg)
	if err != nil {
		t.Fatalf("err: %s", err)
	}
	cons.Ack()
	cons.Close()

	// The first message should now be delivered
	if pri != 0 || msg != "hello0" {
		t.Fatalf("bad: %d: %s", pri, msg)
	}
}

func TestPriorityQueueMinQuietPeriod(t *testing.T) {
	b := inmem.NewInmemBroker()

	// Create a priority queue with a quiet period below the minimum
	q, err := NewPriorityQueue(b, 1, "testing", 0)
	if err != nil {
		t.Fatalf("err: %s", err)
	}

	if err := q.Publish("hello", 0); err != nil {
		t.Fatalf("err: %s", err)
	}

	start := time.Now()

	var msg string
	cons, _, err := q.Consume(&msg)
	if err != nil {
		t.Fatalf("err: %s", err)
	}
	defer cons.Close()

	end := time.Now()

	if end.Sub(start) < MinQuietPeriod {
		t.Fatalf("MinQuietPeriod was not honored")
	}
}

func TestPriorityQueueInterrupt(t *testing.T) {
	b := inmem.NewInmemBroker()

	q, err := NewPriorityQueue(b, 1, "testing", MinQuietPeriod)
	if err != nil {
		t.Fatalf("err: %s", err)
	}

	var msg string
	cancelCh := make(chan struct{})
	doneCh := make(chan struct{})

	go func(doneCh chan struct{}) {
		q.ConsumeCancel(&msg, cancelCh)
		close(doneCh)
	}(doneCh)

	go func() {
		time.Sleep(10 * time.Millisecond)
		close(cancelCh)
	}()

	select {
	case <-doneCh:
		return
	case <-time.After(time.Second):
		t.Fatalf("should have been cancelled")
	}
}
