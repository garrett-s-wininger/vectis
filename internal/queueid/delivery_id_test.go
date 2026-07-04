package queueid

import "testing"

func TestEncodeDecode(t *testing.T) {
	id := Encode("queue:50051/blue", "delivery-1")

	instanceID, token, ok := Decode(id)
	if !ok {
		t.Fatalf("expected encoded delivery id to decode: %q", id)
	}

	if instanceID != "queue:50051/blue" || token != "delivery-1" {
		t.Fatalf("decoded mismatch: instance=%q token=%q", instanceID, token)
	}
}

func TestDecodeLegacyDeliveryID(t *testing.T) {
	instanceID, token, ok := Decode("delivery-1")
	if ok {
		t.Fatal("expected legacy delivery id to report ok=false")
	}

	if instanceID != "" || token != "delivery-1" {
		t.Fatalf("legacy decode mismatch: instance=%q token=%q", instanceID, token)
	}
}
