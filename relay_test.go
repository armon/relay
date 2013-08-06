package relay

import (
	"bytes"
	"os"
	"reflect"
	"sync"
	"testing"
	"time"
)

func IsInteg() bool {
	return os.Getenv("INTEG_TESTS") == "true" && AMQPHost() != ""
}

func AMQPHost() string {
	return os.Getenv("AMQP_HOST")
}

func testSendRecv(t *testing.T, r *Relay) {
	// Get a publisher
	pub, err := r.Publisher("test")
	if err != nil {
		t.Fatalf("unexpected err %s", err)
	}
	defer pub.Close()

	// Get a consumer
	cons, err := r.Consumer("test")
	if err != nil {
		t.Fatalf("unexpected err %s", err)
	}
	defer cons.Close()

	// Send a message
	msg := "the quick brown fox jumps over the lazy dog"
	err = pub.Publish(msg)
	if err != nil {
		t.Fatalf("unexpected err %s", err)
	}

	// Try to get the message
	var in string
	err = cons.Consume(&in)
	if err != nil {
		t.Fatalf("unexpected err %s", err)
	}

	// Ack the message
	err = cons.Ack()
	if err != nil {
		t.Fatalf("unexpected err %s", err)
	}

	// Check message
	if in != msg {
		t.Fatalf("unexpected msg! %v %v", in, msg)
	}
}

func TestSimplePublishConsume(t *testing.T) {
	if !IsInteg() {
		t.SkipNow()
	}

	conf := Config{Addr: AMQPHost()}
	r, err := New(&conf)
	if err != nil {
		t.Fatalf("unexpected err %s", err)
	}
	defer r.Close()

	testSendRecv(t, r)
}

func TestPublishNoConfirm(t *testing.T) {
	if !IsInteg() {
		t.SkipNow()
	}

	conf := Config{Addr: AMQPHost(), DisablePublishConfirm: true}
	r, err := New(&conf)
	if err != nil {
		t.Fatalf("unexpected err %s", err)
	}
	defer r.Close()

	testSendRecv(t, r)
}

func TestPublishNoPersist(t *testing.T) {
	if !IsInteg() {
		t.SkipNow()
	}

	conf := Config{Addr: AMQPHost(), DisablePersistence: true}
	r, err := New(&conf)
	if err != nil {
		t.Fatalf("unexpected err %s", err)
	}
	defer r.Close()

	testSendRecv(t, r)
}

func TestCustomExchange(t *testing.T) {
	if !IsInteg() {
		t.SkipNow()
	}

	conf := Config{Addr: AMQPHost(), Exchange: "my-exchange"}
	r, err := New(&conf)
	if err != nil {
		t.Fatalf("unexpected err %s", err)
	}
	defer r.Close()

	testSendRecv(t, r)
}

func TestRelayMultiClose(t *testing.T) {
	if !IsInteg() {
		t.SkipNow()
	}

	conf := Config{Addr: AMQPHost()}
	r, err := New(&conf)
	if err != nil {
		t.Fatalf("unexpected err %s", err)
	}
	if err := r.Close(); err != nil {
		t.Fatalf("unexpected err")
	}
	if err := r.Close(); err != nil {
		t.Fatalf("unexpected err")
	}
}

func TestConsumerMultiClose(t *testing.T) {
	if !IsInteg() {
		t.SkipNow()
	}

	conf := Config{Addr: AMQPHost()}
	r, err := New(&conf)
	if err != nil {
		t.Fatalf("unexpected err %s", err)
	}

	cons, err := r.Consumer("test")
	if err != nil {
		t.Fatalf("unexpected err %s", err)
	}

	if err := cons.Close(); err != nil {
		t.Fatalf("unexpected err")
	}
	if err := cons.Close(); err != nil {
		t.Fatalf("unexpected err")
	}
}

func TestPublisherMultiClose(t *testing.T) {
	if !IsInteg() {
		t.SkipNow()
	}

	conf := Config{Addr: AMQPHost()}
	r, err := New(&conf)
	if err != nil {
		t.Fatalf("unexpected err %s", err)
	}

	pub, err := r.Publisher("test")
	if err != nil {
		t.Fatalf("unexpected err %s", err)
	}

	if err := pub.Close(); err != nil {
		t.Fatalf("unexpected err")
	}
	if err := pub.Close(); err != nil {
		t.Fatalf("unexpected err")
	}
}

func TestMultiConsume(t *testing.T) {
	if !IsInteg() {
		t.SkipNow()
	}

	conf := Config{Addr: AMQPHost(), PrefetchCount: 5, EnableMultiAck: true}
	r, err := New(&conf)
	if err != nil {
		t.Fatalf("unexpected err %s", err)
	}
	defer r.Close()

	// Get a publisher
	pub, err := r.Publisher("test")
	if err != nil {
		t.Fatalf("unexpected err %s", err)
	}
	defer pub.Close()

	// Get a consumer
	cons, err := r.Consumer("test")
	if err != nil {
		t.Fatalf("unexpected err %s", err)
	}
	defer cons.Close()

	// Send a message
	for i := 0; i < 5; i++ {
		err = pub.Publish(string(i))
		if err != nil {
			t.Fatalf("unexpected err %s", err)
		}
	}

	// Try to get the message
	var in string
	for i := 0; i < 5; i++ {
		err = cons.Consume(&in)
		if err != nil {
			t.Fatalf("unexpected err %s", err)
		}
		if in != string(i) {
			t.Fatalf("unexpected msg! %v %v", in, i)
		}
	}

	// Nack all the messages
	err = cons.Nack()
	if err != nil {
		t.Fatalf("unexpected err %s", err)
	}

	// Should redeliver
	for i := 0; i < 5; i++ {
		err = cons.ConsumeAck(&in)
		if err != nil {
			t.Fatalf("unexpected err %s", err)
		}
		if in != string(i) {
			t.Fatalf("unexpected msg! %v %v", in, i)
		}
	}
}

func TestCloseRelayInUse(t *testing.T) {
	if !IsInteg() {
		t.SkipNow()
	}

	conf := Config{Addr: AMQPHost()}
	r, err := New(&conf)
	if err != nil {
		t.Fatalf("unexpected err %s", err)
	}
	defer r.Close()

	// Get a publisher
	pub, err := r.Publisher("close-in-use")
	if err != nil {
		t.Fatalf("unexpected err %s", err)
	}
	defer pub.Close()

	// Get a consumer
	cons, err := r.Consumer("close-in-use")
	if err != nil {
		t.Fatalf("unexpected err %s", err)
	}
	defer cons.Close()

	wg := &sync.WaitGroup{}
	wg.Add(3)

	// Send a message
	go func() {
		defer wg.Done()
		for i := 0; i < 100; i++ {
			err := pub.Publish(string(i))
			if err == ChannelClosed {
				break
			}
			if err != nil {
				t.Fatalf("unexpected err %s", err)
			}
		}
	}()

	// Should redeliver
	go func() {
		defer wg.Done()
		var in string
		for i := 0; i < 100; i++ {
			err := cons.ConsumeAck(&in)
			if err == ChannelClosed {
				break
			}
			if err != nil {
				t.Fatalf("unexpected err %s", err)
			}
			if in != string(i) {
				t.Fatalf("unexpected msg! %v %v", in, i)
			}
		}
	}()

	go func() {
		defer wg.Done()
		time.Sleep(time.Millisecond)
		err := r.Close()
		if err != nil {
			t.Fatalf("unexpected err %s", err)
		}
	}()

	wg.Wait()
}

func TestQueueName(t *testing.T) {
	if queueName("test") != "relay.test" {
		t.Fatalf("bad queue name")
	}
}

func TestChannelName(t *testing.T) {
	names := make(map[string]struct{})
	for i := 0; i < 100; i++ {
		name, err := channelName()
		if err != nil {
			t.Fatalf("unexpected err %s", err)
		}
		if name == "" {
			t.Fatalf("expected name")
		}
		if _, ok := names[name]; ok {
			t.Fatalf("expected unique name!")
		}
		names[name] = struct{}{}
	}
}

func TestGOBSerializer(t *testing.T) {
	type basic struct {
		Key   string
		Value string
		When  time.Time
	}

	g := &GOBSerializer{}

	if g.ContentType() != "binary/gob" {
		t.Fatalf("bad content type")
	}

	obj := basic{"test", "this is a value", time.Now()}
	var buf bytes.Buffer

	// Encode the struct
	if err := g.Encode(&buf, &obj); err != nil {
		t.Fatalf("unexpected err %s", err)
	}

	// Try to decode
	out := basic{}
	if err := g.Decode(&buf, &out); err != nil {
		t.Fatalf("unexpected err %s", err)
	}

	// Ensure equal
	if !reflect.DeepEqual(obj, out) {
		t.Fatalf("not equal. %v %v", obj, out)
	}
}
