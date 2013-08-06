package relay

import (
	"bytes"
	"os"
	"reflect"
	"testing"
	"time"
)

func IsInteg() bool {
	return os.Getenv("INTEG_TESTS") == "true"
}

func AMQPHost() string {
	return os.Getenv("AMQP_HOST")
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

	// Check message
	if in != msg {
		t.Fatalf("unexpected msg! %v %v", in, msg)
	}
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
