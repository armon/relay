package relay

import (
	"bytes"
	"reflect"
	"testing"
	"time"
)

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
	if err := g.RelayEncode(&buf, &obj); err != nil {
		t.Fatalf("unexpected err %s", err)
	}

	// Try to decode
	out := basic{}
	if err := g.RelayDecode(&buf, &out); err != nil {
		t.Fatalf("unexpected err %s", err)
	}

	// Ensure equal
	if !reflect.DeepEqual(obj, out) {
		t.Fatalf("not equal. %v %v", obj, out)
	}
}

func TestJSONSerializer(t *testing.T) {

	type basic struct {
		Key       string    `json:"key"`
		Value     float64   `json:"value"`
		Timestamp time.Time `json:"ts"`
	}

	jsonSerializer := &JSONSerializer{}

	if jsonSerializer.ContentType() != "text/json" {
		t.Fatalf("bad content type")
	}

	obj := basic{"1234567890", 12.1, time.Now().UTC()}

	var buf bytes.Buffer

	// Encode the basic struct to JSON
	if err := jsonSerializer.RelayEncode(&buf, &obj); err != nil {
		t.Fatalf("unexpected err %s", err)
	}

	// Decode the basic struct from JSON
	out := basic{}
	if err := jsonSerializer.RelayDecode(&buf, &out); err != nil {
		t.Fatalf("unexpected err %s", err)
	}

	// Ensure the two encoded and decoded values are equal
	if !reflect.DeepEqual(obj, out) {
		t.Fatalf("not equal. %#v %#v", obj, out)
	}

}
