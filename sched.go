// Command packets simulates start-time fair queuing (SFQ) of multiple
// packet streams over a single non-preemptive link.
//
// The program reads a JSON problem from stdin (or a file argument) and
// writes a JSON schedule to stdout. See README.md for the format.
package main

import (
	"fmt"
	"math/big"
)

// Stream is one flow: a unique printable-ASCII id and a weight in [1,10].
type Stream struct {
	ID     string `json:"id"`
	Weight int64  `json:"weight"`
}

// WeightChange changes one stream's configured weight at an integer instant.
type WeightChange struct {
	At     int64  `json:"at"`
	Stream string `json:"stream"`
	Weight int64  `json:"weight"`
}

// PacketIn is one packet of the input trace. Packets arrive in
// non-decreasing arrival order; ties keep input order, and packets of the
// same stream keep this order (per-stream FIFO).
type PacketIn struct {
	ID       string `json:"id"`
	Stream   string `json:"stream"`
	Arrival  int64  `json:"arrival"`
	Duration int64  `json:"duration"`
}

// Input is the whole scheduling problem.
type Input struct {
	Streams       []Stream       `json:"streams"`
	Packets       []PacketIn     `json:"packets"`
	WeightChanges []WeightChange `json:"weight_changes,omitempty"`
}

// PacketOut reports the (reduced) tags and the real transmission window
// of one packet. Tags are exact rationals rendered by big.Rat.RatString,
// e.g. "3", "101/10".
type PacketOut struct {
	ID        string `json:"id"`
	Stream    string `json:"stream"`
	Arrival   int64  `json:"arrival"`
	Duration  int64  `json:"duration"`
	StartTag  string `json:"start_tag"`
	FinishTag string `json:"finish_tag"`
	Start     int64  `json:"start"`
	End       int64  `json:"end"`
}

// Output is the full simulation result: per-packet tags and real
// start/end times, the transmission order, the idle intervals [start,end)
// of the link, and the per-stream completion order.
type Output struct {
	Packets    []PacketOut         `json:"packets"`
	Schedule   []string            `json:"schedule"`
	Idle       [][2]int64          `json:"idle"`
	Completion map[string][]string `json:"completion"`
}

const (
	minStreams  = 2
	maxStreams  = 20
	maxPackets  = 500
	minWeight   = 1
	maxWeight   = 10
	minDuration = 1
	maxDuration = 1000
)

// isASCIIID reports whether s is a non-empty printable-ASCII string
// without spaces.
func isASCIIID(s string) bool {
	if s == "" {
		return false
	}
	for i := 0; i < len(s); i++ {
		if s[i] < 0x21 || s[i] > 0x7e {
			return false
		}
	}
	return true
}

// validate checks the input against the problem constraints.
func validate(in *Input) error {
	if len(in.Streams) < minStreams || len(in.Streams) > maxStreams {
		return fmt.Errorf("streams: need %d-%d, got %d", minStreams, maxStreams, len(in.Streams))
	}
	streams := make(map[string]bool, len(in.Streams))
	for _, s := range in.Streams {
		if !isASCIIID(s.ID) {
			return fmt.Errorf("stream id %q: want non-empty printable ASCII", s.ID)
		}
		if streams[s.ID] {
			return fmt.Errorf("duplicate stream id %q", s.ID)
		}
		streams[s.ID] = true
		if s.Weight < minWeight || s.Weight > maxWeight {
			return fmt.Errorf("stream %q: weight %d out of [%d,%d]", s.ID, s.Weight, minWeight, maxWeight)
		}
	}
	if len(in.Packets) > maxPackets {
		return fmt.Errorf("packets: at most %d, got %d", maxPackets, len(in.Packets))
	}
	ids := make(map[string]bool, len(in.Packets))
	var prev int64
	for i, p := range in.Packets {
		if !isASCIIID(p.ID) {
			return fmt.Errorf("packet #%d: id %q: want non-empty printable ASCII", i, p.ID)
		}
		if ids[p.ID] {
			return fmt.Errorf("duplicate packet id %q", p.ID)
		}
		ids[p.ID] = true
		if !streams[p.Stream] {
			return fmt.Errorf("packet %q: unknown stream %q", p.ID, p.Stream)
		}
		if p.Arrival < 0 {
			return fmt.Errorf("packet %q: negative arrival %d", p.ID, p.Arrival)
		}
		if i > 0 && p.Arrival < prev {
			return fmt.Errorf("packet %q: arrivals must be non-decreasing", p.ID)
		}
		prev = p.Arrival
		if p.Duration < minDuration || p.Duration > maxDuration {
			return fmt.Errorf("packet %q: duration %d out of [%d,%d]", p.ID, p.Duration, minDuration, maxDuration)
		}
	}
	seenChange := make(map[struct {
		at     int64
		stream string
	}]bool, len(in.WeightChanges))
	var prevChangeAt int64
	for _, change := range in.WeightChanges {
		if !streams[change.Stream] || change.At < 0 || change.Weight < minWeight || change.Weight > maxWeight {
			return fmt.Errorf("invalid weight change for stream %q", change.Stream)
		}
		if change.At < prevChangeAt {
			return fmt.Errorf("weight change for stream %q: times must be non-decreasing", change.Stream)
		}
		prevChangeAt = change.At
		key := struct {
			at     int64
			stream string
		}{change.At, change.Stream}
		if seenChange[key] {
			return fmt.Errorf("duplicate weight change for stream %q at %d", change.Stream, change.At)
		}
		seenChange[key] = true
	}
	return nil
}

// pkt is a packet with its assigned tags and transmission window.
type pkt struct {
	PacketIn
	startTag  *big.Rat
	finishTag *big.Rat
	start     int64
	end       int64
}

// lessPkt orders eligible packets by (finish tag, stream id, packet id).
func lessPkt(a, b *pkt) bool {
	if c := a.finishTag.Cmp(b.finishTag); c != 0 {
		return c < 0
	}
	if a.Stream != b.Stream {
		return a.Stream < b.Stream
	}
	return a.ID < b.ID
}

// schedule runs the simulation.
//
// Virtual time V and every stream's last assigned/started finish tags
// start at zero. On arrival a packet gets S = max(previous finish tag of
// its stream, V) and F = S + duration/weight. Whenever the link is idle,
// the head-of-queue packet with the smallest (F, stream id, packet id)
// among arrived packets is transmitted non-preemptively and V is set to
// its F. At a weight change, all of that stream's queued packets are
// retagged from max(finish tag of the stream's last started packet, V),
// using the new weight. Completions, weight changes, and arrivals at one
// instant are all processed before the next selection; weight changes
// precede arrivals, so new packets use the new weight.
func schedule(in *Input) *Output {
	weight := make(map[string]int64, len(in.Streams))
	for _, s := range in.Streams {
		weight[s.ID] = s.Weight
	}

	pkts := make([]*pkt, len(in.Packets))
	for i, p := range in.Packets {
		pkts[i] = &pkt{PacketIn: p}
	}

	out := &Output{
		Packets:    make([]PacketOut, 0, len(pkts)),
		Schedule:   make([]string, 0, len(pkts)),
		Idle:       make([][2]int64, 0),
		Completion: make(map[string][]string, len(in.Streams)),
	}
	for _, s := range in.Streams {
		out.Completion[s.ID] = []string{}
	}

	v := new(big.Rat)                     // global virtual time
	lastF := make(map[string]*big.Rat)    // finish tag of last assigned packet per stream
	startedF := make(map[string]*big.Rat) // finish tag of last started packet per stream
	queues := make(map[string][]*pkt)     // arrived, waiting packets per stream

	var t int64           // current time
	next := 0             // index of the next arrival
	nextChange := 0       // index of the next weight change
	var cur *pkt          // packet in transmission, nil while idle
	idleSince := int64(0) // start of the current idle period; -1 while busy

	// assign tags to one newly arrived packet using its stream's current weight.
	assign := func(p *pkt) {
		s := new(big.Rat).Set(v)
		if f := lastF[p.Stream]; f != nil && f.Cmp(s) > 0 {
			s.Set(f)
		}
		p.startTag = s
		p.finishTag = new(big.Rat).Add(s, big.NewRat(p.Duration, weight[p.Stream]))
		lastF[p.Stream] = p.finishTag
		queues[p.Stream] = append(queues[p.Stream], p)
	}

	// reprice all queued packets of stream id after a weight change. The
	// packet currently in transmission (if any) keeps its old tags and is
	// the anchor for the waiting FIFO chain.
	reprice := func(id string) {
		q := queues[id]
		if len(q) == 0 {
			return
		}
		base := new(big.Rat).Set(v)
		if f := startedF[id]; f != nil && f.Cmp(base) > 0 {
			base.Set(f)
		}
		for _, p := range q {
			p.startTag = new(big.Rat).Set(base)
			p.finishTag = new(big.Rat).Add(base, big.NewRat(p.Duration, weight[id]))
			base = p.finishTag
		}
		lastF[id] = q[len(q)-1].finishTag
	}

	for {
		if cur == nil {
			// Link is idle: pick the best head-of-queue packet.
			var best *pkt
			for _, s := range in.Streams {
				if q := queues[s.ID]; len(q) > 0 {
					if p := q[0]; best == nil || lessPkt(p, best) {
						best = p
					}
				}
			}
			if best != nil {
				queues[best.Stream] = queues[best.Stream][1:]
				best.start = t
				best.end = t + best.Duration
				v.Set(best.finishTag)
				startedF[best.Stream] = best.finishTag
				cur = best
				out.Schedule = append(out.Schedule, best.ID)
				if idleSince >= 0 && t > idleSince {
					out.Idle = append(out.Idle, [2]int64{idleSince, t})
				}
				idleSince = -1
			} else if next >= len(pkts) && nextChange >= len(in.WeightChanges) {
				break // nothing arrived or pending, nothing left: done
			}
		}

		// Advance to the next event: completion, weight change, or arrival.
		at := int64(-1)
		if cur != nil {
			at = cur.end
		}
		if next < len(pkts) && (at < 0 || pkts[next].Arrival < at) {
			at = pkts[next].Arrival
		}
		if nextChange < len(in.WeightChanges) && (at < 0 || in.WeightChanges[nextChange].At < at) {
			at = in.WeightChanges[nextChange].At
		}
		if at < 0 {
			break
		}
		t = at

		if cur != nil && cur.end == t {
			out.Completion[cur.Stream] = append(out.Completion[cur.Stream], cur.ID)
			cur = nil
			idleSince = t
		}

		// Weight changes at this instant take effect before arrivals. They
		// are ordered by input, but changes to different streams are
		// independent.
		for nextChange < len(in.WeightChanges) && in.WeightChanges[nextChange].At == t {
			change := in.WeightChanges[nextChange]
			weight[change.Stream] = change.Weight
			reprice(change.Stream)
			nextChange++
		}

		// All arrivals at this instant are tagged before any selection.
		for next < len(pkts) && pkts[next].Arrival == t {
			assign(pkts[next])
			next++
		}
	}

	for _, p := range pkts {
		out.Packets = append(out.Packets, PacketOut{
			ID:        p.ID,
			Stream:    p.Stream,
			Arrival:   p.Arrival,
			Duration:  p.Duration,
			StartTag:  p.startTag.RatString(),
			FinishTag: p.finishTag.RatString(),
			Start:     p.start,
			End:       p.end,
		})
	}
	return out
}
