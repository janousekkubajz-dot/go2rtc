package srtp

import (
	"testing"
	"time"

	"github.com/pion/rtp"
)

func TestReorderOutOfOrderAndDuplicate(t *testing.T) {
	delivered := make(chan uint16, 8)
	r := newReorderBuffer(func(p *rtp.Packet) { delivered <- p.SequenceNumber })
	defer r.Close()
	for _, seq := range []uint16{10, 12, 11, 12, 13} {
		r.Push(&rtp.Packet{Header: rtp.Header{SequenceNumber: seq}})
	}
	for _, want := range []uint16{10, 11, 12, 13} {
		select {
		case got := <-delivered:
			if got != want {
				t.Fatalf("want %d, got %d", want, got)
			}
		case <-time.After(2 * time.Second):
			t.Fatalf("timeout waiting for %d", want)
		}
	}
	select {
	case got := <-delivered:
		t.Fatalf("unexpected duplicate delivered: %d", got)
	case <-time.After(100 * time.Millisecond):
	}
}

func TestReorderLargeLossDoesNotFreeze(t *testing.T) {
	delivered := make(chan uint16, 8)
	r := newReorderBuffer(func(p *rtp.Packet) { delivered <- p.SequenceNumber })
	defer r.Close()
	for _, seq := range []uint16{100, 1000, 1001} {
		r.Push(&rtp.Packet{Header: rtp.Header{SequenceNumber: seq}})
	}
	for _, want := range []uint16{100, 1000, 1001} {
		select {
		case got := <-delivered:
			if got != want {
				t.Fatalf("want %d, got %d", want, got)
			}
		case <-time.After(500 * time.Millisecond):
			t.Fatalf("reorder buffer frozen waiting for %d", want)
		}
	}
}

func TestReorderSequenceWrap(t *testing.T) {
	delivered := make(chan uint16, 8)
	r := newReorderBuffer(func(p *rtp.Packet) { delivered <- p.SequenceNumber })
	defer r.Close()
	for _, seq := range []uint16{65534, 0, 65535, 1} {
		r.Push(&rtp.Packet{Header: rtp.Header{SequenceNumber: seq}})
	}
	for _, want := range []uint16{65534, 65535, 0, 1} {
		select {
		case got := <-delivered:
			if got != want {
				t.Fatalf("want %d, got %d", want, got)
			}
		case <-time.After(2 * time.Second):
			t.Fatalf("timeout waiting for %d", want)
		}
	}
}
