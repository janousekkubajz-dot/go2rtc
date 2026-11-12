
package srtp

import (
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
    case r.input <- p:
    case <-r.done:
    }
}

func (r *reorderBuffer) Close() {
    close(r.done)
}

func (r *reorderBuffer) run(deliver func(*rtp.Packet)) {
    ticker := time.NewTicker(10 * time.Millisecond)
    defer ticker.Stop()

    pending := make(map[uint16]*rtp.Packet)
    var expected uint16
    var started bool
    var gapSince time.Time

    flush := func() {
        for {
            if p, ok := pending[expected]; ok {
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

            if len(pending) < reorderDepth &&
                time.Since(gapSince) < reorderDelay {
                return
            }

            expected++
            gapSince = time.Time{}
        }
    }

    for {
        select {
        case p := <-r.input:
            seq := p.SequenceNumber

            if !started {
                expected = seq
                started = true
            }

            diff := int16(seq - expected)
            if diff < 0 {
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

        case <-r.done:
            return
        }
    }
}
