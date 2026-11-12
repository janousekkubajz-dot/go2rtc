package srtp

import (
	"github.com/pion/rtp"
	"sync"
	"testing"
	"time"
)

func TestReorderOutOfOrderAndMissing(t *testing.T) {
	var mu sync.Mutex
	var received []uint16
	r := newReorderBuffer(func(p *rtp.Packet) {
		mu.Lock()
		received = append(received, p.SequenceNumber)
		mu.Unlock()
	})
	defer r.Close()
	for _, n := range []uint16{100, 102, 101, 104, 104} {
		r.Push(&rtp.Packet{Header: rtp.Header{SequenceNumber: n}})
	}
	time.Sleep(130 * time.Millisecond)
	mu.Lock()
	defer mu.Unlock()
	want := []uint16{100, 101, 102, 104}
	if len(received) != len(want) {
		t.Fatalf("received %v; expected %v", received, want)
	}
	for i := range want {
		if received[i] != want[i] {
			t.Fatalf("received %v; expected %v", received, want)
		}
	}
}

func TestReorderWraparound(t *testing.T) {
	var mu sync.Mutex
	var got []uint16
	r := newReorderBuffer(func(p *rtp.Packet) { mu.Lock(); got = append(got, p.SequenceNumber); mu.Unlock() })
	defer r.Close()
	for _, seq := range []uint16{65534, 0, 65535, 1} {
		r.Push(&rtp.Packet{Header: rtp.Header{SequenceNumber: seq}})
	}
	time.Sleep(80 * time.Millisecond)
	mu.Lock()
	defer mu.Unlock()
	expected := []uint16{65534, 65535, 0, 1}
	if len(got) != len(expected) {
		t.Fatalf("got %v", got)
	}
	for i := range expected {
		if got[i] != expected[i] {
			t.Fatalf("got %v", got)
		}
	}
}

func TestReorderCloseTwice(t *testing.T) {
	r := newReorderBuffer(func(*rtp.Packet) {})
	r.Close()
	r.Close()
	r.Push(&rtp.Packet{})
}
