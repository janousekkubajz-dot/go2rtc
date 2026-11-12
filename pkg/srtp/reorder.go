package srtp

import (
	"sync"
	"time"

	"github.com/pion/rtp"
)

const (
	reorderDepth = 32
	reorderDelay = 60 * time.Millisecond
)

type reorderBuffer struct {
	input chan *rtp.Packet
	done  chan struct{}
	once  sync.Once
}

func newReorderBuffer(deliver func(*rtp.Packet)) *reorderBuffer {
	r := &reorderBuffer{
		input: make(chan *rtp.Packet, 1024),
		done:  make(chan struct{}),
	}
	go r.run(deliver)
	return r
}

func (r *reorderBuffer) Push(packet *rtp.Packet) {
	if packet == nil {
		return
	}
	select {
	case <-r.done:
		return
	default:
	}
	select {
	case <-r.done:
	case r.input <- packet:
	}
}

// Close is safe to call more than once. It stops the worker even if no
// packets arrive after the camera has disconnected.
func (r *reorderBuffer) Close() {
	r.once.Do(func() { close(r.done) })
}

func (r *reorderBuffer) run(deliver func(*rtp.Packet)) {
	ticker := time.NewTicker(5 * time.Millisecond)
	defer ticker.Stop()

	pending := make(map[uint16]*rtp.Packet)
	var next uint16
	var started bool
	var gapSince time.Time

	flush := func(now time.Time) {
		for {
			if p, ok := pending[next]; ok {
				delete(pending, next)
				deliver(p)
				next++
				gapSince = time.Time{}
				continue
			}
			if len(pending) == 0 {
				gapSince = time.Time{}
				return
			}
			if gapSince.IsZero() {
				gapSince = now
			}
			if len(pending) < reorderDepth && now.Sub(gapSince) < reorderDelay {
				return
			}
			// Missing packet: move directly to the closest available
			// sequence number, including across the uint16 wraparound.
			bestDistance := uint16(0xffff)
			var bestSeq uint16
			for seq := range pending {
				distance := uint16(seq - next)
				if distance < bestDistance {
					bestDistance, bestSeq = distance, seq
				}
			}
			next = bestSeq
			gapSince = time.Time{}
		}
	}

	for {
		// Prioritize shutdown even when input is continuously busy.
		select {
		case <-r.done:
			return
		default:
		}
		select {
		case <-r.done:
			return
		case p := <-r.input:
			seq := p.SequenceNumber
			if !started {
				next = seq
				started = true
			}
			// A negative signed distance represents an old/duplicate packet.
			// Large jumps are treated as old rather than filling the buffer.
			if int16(seq-next) < 0 {
				continue
			}
			if _, ok := pending[seq]; ok {
				continue
			}
			pending[seq] = p
			flush(time.Now())
		case now := <-ticker.C:
			if started {
				flush(now)
			}
		}
	}
}
