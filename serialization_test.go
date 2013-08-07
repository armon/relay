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
