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

func (r *reorderBuffer) Push(p *rtp.Packet) {
	select {
	case <-r.done:
		return
	default:
	}
	select {
	case r.input <- p:
	case <-r.done:
	}
}

func (r *reorderBuffer) Close() {
	r.once.Do(func() { close(r.done) })
}

func (r *reorderBuffer) run(deliver func(*rtp.Packet)) {
	ticker := time.NewTicker(5 * time.Millisecond)
	defer ticker.Stop()

	pending := make(map[uint16]*rtp.Packet)
	var expected uint16
	var started bool
	var gapSince time.Time

	flush := func() {
		for {
			if p, found := pending[expected]; found {
				delete(pending, expected)
				deliver(p)
				expected++
				gapSince = time.Time{}
				continue
			}
			if len(pending) == 0 {
				gapSince = time.Time{}
				return
			}
			if gapSince.IsZero() {
				gapSince = time.Now()
			}
			if len(pending) < reorderDepth && time.Since(gapSince) < reorderDelay {
				return
			}

			// Jump directly to the closest available forward packet.
			// Skipping a large gap one sequence number per 60 ms
			// would otherwise freeze video for many seconds or minutes.
			found := false
			var closest uint16
			var minDistance uint16
			for seq := range pending {
				d := seq - expected
				if d < 0x8000 && (!found || d < minDistance) {
					closest, minDistance, found = seq, d, true
				}
			}
			if !found {
				// All pending numbers are outside the forward half-window.
				for seq := range pending {
					closest = seq
					break
				}
			}
			expected = closest
			gapSince = time.Time{}
		}
	}

	for {
		select {
		case <-r.done:
			return
		case p := <-r.input:
			if p == nil {
				continue
			}
			seq := p.SequenceNumber
			if !started {
				expected = seq
				started = true
			}
			if int16(seq-expected) < 0 {
				continue
			}
			if _, exists := pending[seq]; exists {
				continue
			}
			pending[seq] = p
			flush()
		case <-ticker.C:
			if started {
				flush()
			}
		}
	}
}
