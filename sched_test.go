package main

import (
	"encoding/json"
	"fmt"
	"math/big"
	"math/rand"
	"reflect"
	"sort"
	"testing"
)

// referenceSchedule is an independent, deliberately straightforward
// event-by-event simulation of the same spec, used to cross-check
// schedule. It scans the whole packet list for head-of-queue candidates
// and sorts them, instead of maintaining per-stream queues.
func referenceSchedule(in *Input) *Output {
	weight := make(map[string]int64, len(in.Streams))
	for _, s := range in.Streams {
		weight[s.ID] = s.Weight
	}

	type rp struct {
		PacketIn
		idx           int
		s, f          *big.Rat
		start, end    int64
		arrived, done bool
	}
	ps := make([]*rp, len(in.Packets))
	for i, p := range in.Packets {
		ps[i] = &rp{PacketIn: p, idx: i}
	}

	out := &Output{
		Packets:    make([]PacketOut, 0, len(ps)),
		Schedule:   make([]string, 0, len(ps)),
		Idle:       make([][2]int64, 0),
		Completion: make(map[string][]string, len(in.Streams)),
	}
	for _, s := range in.Streams {
		out.Completion[s.ID] = []string{}
	}

	v := new(big.Rat)
	lastF := make(map[string]*big.Rat)
	startedF := make(map[string]*big.Rat)
	var t int64
	arrivals := 0 // ps[:arrivals] have been tagged and released
	changes := 0
	var cur *rp
	idleSince := int64(0)

	// assign tags to a packet arriving now.
	assign := func(p *rp) {
		s := new(big.Rat).Set(v)
		if f := lastF[p.Stream]; f != nil && f.Cmp(s) > 0 {
			s.Set(f)
		}
		p.s = s
		p.f = new(big.Rat).Add(s, big.NewRat(p.Duration, weight[p.Stream]))
		lastF[p.Stream] = p.f
		p.arrived = true
	}

	// reprice all arrived, waiting packets of one stream in FIFO order.
	reprice := func(id string) {
		base := new(big.Rat).Set(v)
		if f := startedF[id]; f != nil && f.Cmp(base) > 0 {
			base.Set(f)
		}
		var last *rp
		for _, p := range ps {
			if p.Stream != id || !p.arrived || p.done {
				continue
			}
			if cur != nil && p.Stream == cur.Stream && p.idx == cur.idx {
				continue
			}
			p.s = new(big.Rat).Set(base)
			p.f = new(big.Rat).Add(base, big.NewRat(p.Duration, weight[id]))
			base = p.f
			last = p
		}
		if last != nil {
			lastF[id] = last.f
		}
	}

	for {
		if cur == nil {
			// Head-of-queue candidates: per stream, the earliest input
			// packet that has arrived and not yet completed.
			head := make(map[string]*rp)
			for _, p := range ps {
				if !p.arrived || p.done {
					continue
				}
				if _, ok := head[p.Stream]; !ok {
					head[p.Stream] = p
				}
			}
			elig := make([]*rp, 0, len(head))
			for _, p := range head {
				elig = append(elig, p)
			}
			if len(elig) > 0 {
				sort.Slice(elig, func(i, j int) bool {
					a, b := elig[i], elig[j]
					if c := a.f.Cmp(b.f); c != 0 {
						return c < 0
					}
					if a.Stream != b.Stream {
						return a.Stream < b.Stream
					}
					return a.ID < b.ID
				})
				p := elig[0]
				if idleSince >= 0 && t > idleSince {
					out.Idle = append(out.Idle, [2]int64{idleSince, t})
				}
				idleSince = -1
				p.start = t
				p.end = t + p.Duration
				v.Set(p.f)
				startedF[p.Stream] = p.f
				cur = p
				out.Schedule = append(out.Schedule, p.ID)
			} else {
				if arrivals >= len(ps) && changes >= len(in.WeightChanges) {
					break
				}
				// Empty system: jump to the next arrival or change.
				nt := int64(-1)
				if arrivals < len(ps) {
					nt = ps[arrivals].Arrival
				}
				if changes < len(in.WeightChanges) && (nt < 0 || in.WeightChanges[changes].At < nt) {
					nt = in.WeightChanges[changes].At
				}
				t = nt
				for changes < len(in.WeightChanges) && in.WeightChanges[changes].At == t {
					change := in.WeightChanges[changes]
					weight[change.Stream] = change.Weight
					reprice(change.Stream)
					changes++
				}
				for arrivals < len(ps) && ps[arrivals].Arrival == t {
					assign(ps[arrivals])
					arrivals++
				}
			}
			continue
		}

		// Transmitting: advance to the next completion, change, or arrival.
		nt := cur.end
		if arrivals < len(ps) && ps[arrivals].Arrival < nt {
			nt = ps[arrivals].Arrival
		}
		if changes < len(in.WeightChanges) && in.WeightChanges[changes].At < nt {
			nt = in.WeightChanges[changes].At
		}
		t = nt
		if cur.end == t {
			out.Completion[cur.Stream] = append(out.Completion[cur.Stream], cur.ID)
			cur.done = true
			cur = nil
			idleSince = t
		}
		for changes < len(in.WeightChanges) && in.WeightChanges[changes].At == t {
			change := in.WeightChanges[changes]
			weight[change.Stream] = change.Weight
			reprice(change.Stream)
			changes++
		}
		for arrivals < len(ps) && ps[arrivals].Arrival == t {
			assign(ps[arrivals])
			arrivals++
		}
	}

	for _, p := range ps {
		out.Packets = append(out.Packets, PacketOut{
			ID:        p.ID,
			Stream:    p.Stream,
			Arrival:   p.Arrival,
			Duration:  p.Duration,
			StartTag:  p.s.RatString(),
			FinishTag: p.f.RatString(),
			Start:     p.start,
			End:       p.end,
		})
	}
	return out
}

// checkOutput verifies spec invariants that must hold for any input.
func checkOutput(t *testing.T, in *Input, out *Output) {
	t.Helper()
	weight := make(map[string]int64, len(in.Streams))
	for _, s := range in.Streams {
		weight[s.ID] = s.Weight
	}

	if len(out.Packets) != len(in.Packets) {
		t.Fatalf("got %d packet records, want %d", len(out.Packets), len(in.Packets))
	}
	weightAt := func(stream string, at int64) int64 {
		w := weight[stream]
		for _, change := range in.WeightChanges {
			if change.Stream == stream && change.At <= at {
				w = change.Weight
			}
		}
		return w
	}

	byID := make(map[string]PacketOut, len(out.Packets))
	lastF := make(map[string]*big.Rat)
	for i, p := range out.Packets {
		q := in.Packets[i]
		if p.ID != q.ID || p.Stream != q.Stream || p.Arrival != q.Arrival || p.Duration != q.Duration {
			t.Errorf("packet #%d: record %+v does not echo input %+v", i, p, q)
		}
		byID[p.ID] = p
		if p.Start < p.Arrival {
			t.Errorf("%s: start %d before arrival %d", p.ID, p.Start, p.Arrival)
		}
		if p.End-p.Start != p.Duration {
			t.Errorf("%s: end-start = %d, want duration %d", p.ID, p.End-p.Start, p.Duration)
		}
		s, ok1 := new(big.Rat).SetString(p.StartTag)
		f, ok2 := new(big.Rat).SetString(p.FinishTag)
		if !ok1 || !ok2 {
			t.Errorf("%s: unparseable tags S=%q F=%q", p.ID, p.StartTag, p.FinishTag)
			continue
		}
		// Tags must be normalized (reduced) rationals.
		if s.RatString() != p.StartTag || f.RatString() != p.FinishTag {
			t.Errorf("%s: tags S=%q F=%q not in reduced form", p.ID, p.StartTag, p.FinishTag)
		}
		// F = S + duration/weight in effect when the packet starts.
		if want := new(big.Rat).Add(s, big.NewRat(p.Duration, weightAt(p.Stream, p.Start))); f.Cmp(want) != 0 {
			t.Errorf("%s: F=%s, want S+D/w=%s", p.ID, f, want)
		}
		// S >= previous finish tag of the same stream.
		if pf, ok := lastF[p.Stream]; ok && s.Cmp(pf) < 0 {
			t.Errorf("%s: S=%s below previous finish tag %s of stream %s", p.ID, s, pf, p.Stream)
		}
		lastF[p.Stream] = f
	}

	// The schedule lists every packet exactly once and is work-conserving:
	// each packet starts at max(its arrival, the previous packet's end).
	if len(out.Schedule) != len(in.Packets) {
		t.Fatalf("schedule has %d entries, want %d", len(out.Schedule), len(in.Packets))
	}
	seen := make(map[string]bool, len(out.Schedule))
	var prevEnd int64
	for i, id := range out.Schedule {
		p, ok := byID[id]
		if !ok {
			t.Fatalf("schedule references unknown packet %q", id)
		}
		if seen[id] {
			t.Errorf("schedule repeats packet %q", id)
		}
		seen[id] = true
		want := p.Arrival
		if i > 0 && prevEnd > want {
			want = prevEnd
		}
		if p.Start != want {
			t.Errorf("%s: start %d, want max(arrival, prevEnd) = %d", id, p.Start, want)
		}
		prevEnd = p.End
	}

	// Idle intervals are exactly the gaps of the busy schedule (from t=0).
	var wantIdle [][2]int64
	prevEnd = 0
	for i, id := range out.Schedule {
		p := byID[id]
		if i == 0 {
			if p.Start > 0 {
				wantIdle = append(wantIdle, [2]int64{0, p.Start})
			}
		} else if p.Start > prevEnd {
			wantIdle = append(wantIdle, [2]int64{prevEnd, p.Start})
		}
		prevEnd = p.End
	}
	if wantIdle == nil {
		wantIdle = [][2]int64{}
	}
	if !reflect.DeepEqual(out.Idle, wantIdle) {
		t.Errorf("idle = %v, want %v", out.Idle, wantIdle)
	}

	// Per-stream completion order equals per-stream input order (FIFO).
	wantComp := make(map[string][]string, len(in.Streams))
	for _, s := range in.Streams {
		wantComp[s.ID] = []string{}
	}
	for _, p := range in.Packets {
		wantComp[p.Stream] = append(wantComp[p.Stream], p.ID)
	}
	if !reflect.DeepEqual(out.Completion, wantComp) {
		t.Errorf("completion = %v, want %v", out.Completion, wantComp)
	}
}

// Hand-computed cases: empty-queue time jumps, ties across different
// weights, late arrivals during transmission, arrivals at the exact
// completion instant, and stream-id tie-breaking.
func TestGolden(t *testing.T) {
	cases := []struct {
		name string
		in   Input
		want Output
	}{
		{
			name: "empty queue jumps time",
			in: Input{
				Streams: []Stream{{ID: "a", Weight: 1}, {ID: "b", Weight: 1}},
				Packets: []PacketIn{
					{ID: "p1", Stream: "a", Arrival: 5, Duration: 2},
					{ID: "p2", Stream: "b", Arrival: 10, Duration: 3},
				},
			},
			want: Output{
				Packets: []PacketOut{
					{ID: "p1", Stream: "a", Arrival: 5, Duration: 2, StartTag: "0", FinishTag: "2", Start: 5, End: 7},
					{ID: "p2", Stream: "b", Arrival: 10, Duration: 3, StartTag: "2", FinishTag: "5", Start: 10, End: 13},
				},
				Schedule:   []string{"p1", "p2"},
				Idle:       [][2]int64{{0, 5}, {7, 10}},
				Completion: map[string][]string{"a": {"p1"}, "b": {"p2"}},
			},
		},
		{
			name: "tie across different weights",
			in: Input{
				Streams: []Stream{{ID: "a", Weight: 2}, {ID: "b", Weight: 1}},
				Packets: []PacketIn{
					{ID: "p1", Stream: "a", Arrival: 0, Duration: 4}, // F = 0+4/2 = 2
					{ID: "p2", Stream: "b", Arrival: 0, Duration: 2}, // F = 0+2/1 = 2
					{ID: "p3", Stream: "a", Arrival: 1, Duration: 2}, // S = max(2,V=2) = 2, F = 3
				},
			},
			want: Output{
				Packets: []PacketOut{
					{ID: "p1", Stream: "a", Arrival: 0, Duration: 4, StartTag: "0", FinishTag: "2", Start: 0, End: 4},
					{ID: "p2", Stream: "b", Arrival: 0, Duration: 2, StartTag: "0", FinishTag: "2", Start: 4, End: 6},
					{ID: "p3", Stream: "a", Arrival: 1, Duration: 2, StartTag: "2", FinishTag: "3", Start: 6, End: 8},
				},
				Schedule:   []string{"p1", "p2", "p3"},
				Idle:       [][2]int64{},
				Completion: map[string][]string{"a": {"p1", "p3"}, "b": {"p2"}},
			},
		},
		{
			name: "late arrival does not preempt",
			in: Input{
				Streams: []Stream{{ID: "a", Weight: 1}, {ID: "b", Weight: 10}},
				Packets: []PacketIn{
					{ID: "p1", Stream: "a", Arrival: 0, Duration: 10},
					{ID: "p2", Stream: "b", Arrival: 1, Duration: 1}, // S = V = 10, F = 101/10
				},
			},
			want: Output{
				Packets: []PacketOut{
					{ID: "p1", Stream: "a", Arrival: 0, Duration: 10, StartTag: "0", FinishTag: "10", Start: 0, End: 10},
					{ID: "p2", Stream: "b", Arrival: 1, Duration: 1, StartTag: "10", FinishTag: "101/10", Start: 10, End: 11},
				},
				Schedule:   []string{"p1", "p2"},
				Idle:       [][2]int64{},
				Completion: map[string][]string{"a": {"p1"}, "b": {"p2"}},
			},
		},
		{
			name: "arrival at the completion instant",
			in: Input{
				Streams: []Stream{{ID: "a", Weight: 1}, {ID: "b", Weight: 1}},
				Packets: []PacketIn{
					{ID: "p1", Stream: "a", Arrival: 0, Duration: 2},
					{ID: "p2", Stream: "b", Arrival: 2, Duration: 1},
				},
			},
			want: Output{
				Packets: []PacketOut{
					{ID: "p1", Stream: "a", Arrival: 0, Duration: 2, StartTag: "0", FinishTag: "2", Start: 0, End: 2},
					{ID: "p2", Stream: "b", Arrival: 2, Duration: 1, StartTag: "2", FinishTag: "3", Start: 2, End: 3},
				},
				Schedule:   []string{"p1", "p2"},
				Idle:       [][2]int64{},
				Completion: map[string][]string{"a": {"p1"}, "b": {"p2"}},
			},
		},
		{
			name: "finish-tag tie broken by stream id",
			in: Input{
				Streams: []Stream{{ID: "a", Weight: 1}, {ID: "b", Weight: 1}},
				Packets: []PacketIn{
					{ID: "p1", Stream: "a", Arrival: 0, Duration: 5},
					{ID: "p2", Stream: "b", Arrival: 1, Duration: 1}, // S = V = 5, F = 6
					{ID: "p3", Stream: "a", Arrival: 2, Duration: 1}, // S = max(5,V=5) = 5, F = 6
				},
			},
			want: Output{
				Packets: []PacketOut{
					{ID: "p1", Stream: "a", Arrival: 0, Duration: 5, StartTag: "0", FinishTag: "5", Start: 0, End: 5},
					{ID: "p2", Stream: "b", Arrival: 1, Duration: 1, StartTag: "5", FinishTag: "6", Start: 6, End: 7},
					{ID: "p3", Stream: "a", Arrival: 2, Duration: 1, StartTag: "5", FinishTag: "6", Start: 5, End: 6},
				},
				Schedule:   []string{"p1", "p3", "p2"},
				Idle:       [][2]int64{},
				Completion: map[string][]string{"a": {"p1", "p3"}, "b": {"p2"}},
			},
		},
		{
			name: "backlogged packet repriced and reordered",
			in: Input{
				Streams: []Stream{{ID: "a", Weight: 1}, {ID: "b", Weight: 1}},
				Packets: []PacketIn{
					{ID: "p1", Stream: "a", Arrival: 0, Duration: 4},
					{ID: "p2", Stream: "a", Arrival: 1, Duration: 4},
					{ID: "q", Stream: "b", Arrival: 1, Duration: 2},
				},
				WeightChanges: []WeightChange{{At: 1, Stream: "a", Weight: 4}},
			},
			want: Output{
				Packets: []PacketOut{
					{ID: "p1", Stream: "a", Arrival: 0, Duration: 4, StartTag: "0", FinishTag: "4", Start: 0, End: 4},
					{ID: "p2", Stream: "a", Arrival: 1, Duration: 4, StartTag: "4", FinishTag: "5", Start: 4, End: 8},
					{ID: "q", Stream: "b", Arrival: 1, Duration: 2, StartTag: "4", FinishTag: "6", Start: 8, End: 10},
				},
				Schedule:   []string{"p1", "p2", "q"},
				Idle:       [][2]int64{},
				Completion: map[string][]string{"a": {"p1", "p2"}, "b": {"q"}},
			},
		},
		{
			name: "change during transmission only affects queued packets",
			in: Input{
				Streams: []Stream{{ID: "a", Weight: 1}, {ID: "b", Weight: 1}},
				Packets: []PacketIn{
					{ID: "p1", Stream: "a", Arrival: 0, Duration: 4},
					{ID: "p2", Stream: "a", Arrival: 1, Duration: 4},
					{ID: "q", Stream: "b", Arrival: 1, Duration: 2},
				},
				WeightChanges: []WeightChange{{At: 2, Stream: "a", Weight: 4}},
			},
			want: Output{
				Packets: []PacketOut{
					{ID: "p1", Stream: "a", Arrival: 0, Duration: 4, StartTag: "0", FinishTag: "4", Start: 0, End: 4},
					{ID: "p2", Stream: "a", Arrival: 1, Duration: 4, StartTag: "4", FinishTag: "5", Start: 4, End: 8},
					{ID: "q", Stream: "b", Arrival: 1, Duration: 2, StartTag: "4", FinishTag: "6", Start: 8, End: 10},
				},
				Schedule:   []string{"p1", "p2", "q"},
				Idle:       [][2]int64{},
				Completion: map[string][]string{"a": {"p1", "p2"}, "b": {"q"}},
			},
		},
		{
			name: "change at completion instant precedes arrival",
			in: Input{
				Streams: []Stream{{ID: "a", Weight: 1}, {ID: "b", Weight: 1}},
				Packets: []PacketIn{
					{ID: "p1", Stream: "a", Arrival: 0, Duration: 4},
					{ID: "p2", Stream: "a", Arrival: 4, Duration: 4},
					{ID: "q", Stream: "b", Arrival: 4, Duration: 2},
				},
				WeightChanges: []WeightChange{{At: 4, Stream: "a", Weight: 4}},
			},
			want: Output{
				Packets: []PacketOut{
					{ID: "p1", Stream: "a", Arrival: 0, Duration: 4, StartTag: "0", FinishTag: "4", Start: 0, End: 4},
					{ID: "p2", Stream: "a", Arrival: 4, Duration: 4, StartTag: "4", FinishTag: "5", Start: 4, End: 8},
					{ID: "q", Stream: "b", Arrival: 4, Duration: 2, StartTag: "4", FinishTag: "6", Start: 8, End: 10},
				},
				Schedule:   []string{"p1", "p2", "q"},
				Idle:       [][2]int64{},
				Completion: map[string][]string{"a": {"p1", "p2"}, "b": {"q"}},
			},
		},
		{
			name: "idle change affects the next arrival batch",
			in: Input{
				Streams: []Stream{{ID: "a", Weight: 1}, {ID: "b", Weight: 1}},
				Packets: []PacketIn{
					{ID: "p", Stream: "a", Arrival: 5, Duration: 4},
					{ID: "q", Stream: "b", Arrival: 5, Duration: 2},
				},
				WeightChanges: []WeightChange{{At: 2, Stream: "a", Weight: 4}},
			},
			want: Output{
				Packets: []PacketOut{
					{ID: "p", Stream: "a", Arrival: 5, Duration: 4, StartTag: "0", FinishTag: "1", Start: 5, End: 9},
					{ID: "q", Stream: "b", Arrival: 5, Duration: 2, StartTag: "0", FinishTag: "2", Start: 9, End: 11},
				},
				Schedule:   []string{"p", "q"},
				Idle:       [][2]int64{{0, 5}},
				Completion: map[string][]string{"a": {"p"}, "b": {"q"}},
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if err := validate(&tc.in); err != nil {
				t.Fatalf("validate: %v", err)
			}
			got := schedule(&tc.in)
			if !reflect.DeepEqual(got, &tc.want) {
				gj, _ := json.MarshalIndent(got, "", "  ")
				wj, _ := json.MarshalIndent(&tc.want, "", "  ")
				t.Errorf("schedule mismatch\ngot:  %s\nwant: %s", gj, wj)
			}
			if ref := referenceSchedule(&tc.in); !reflect.DeepEqual(got, ref) {
				rj, _ := json.MarshalIndent(ref, "", "  ")
				gj, _ := json.MarshalIndent(got, "", "  ")
				t.Errorf("reference disagrees\ngot: %s\nref: %s", gj, rj)
			}
			checkOutput(t, &tc.in, got)
		})
	}
}

// randomInput builds a random valid problem: 2..6 streams, up to 30
// packets, clustered arrivals with occasional large gaps.
func randomInput(rng *rand.Rand) *Input {
	ns := 2 + rng.Intn(5)
	streams := make([]Stream, ns)
	for i := range streams {
		streams[i] = Stream{ID: string(rune('a' + i)), Weight: int64(1 + rng.Intn(10))}
	}
	pkts := make([]PacketIn, rng.Intn(31))
	var at int64
	for i := range pkts {
		if rng.Intn(6) == 0 {
			at += int64(rng.Intn(30)) // gap: forces empty-queue jumps
		}
		at += int64(rng.Intn(4))
		pkts[i] = PacketIn{
			ID:       fmt.Sprintf("p%d", i),
			Stream:   streams[rng.Intn(ns)].ID,
			Arrival:  at,
			Duration: 1 + int64(rng.Intn(20)),
		}
	}
	changes := make([]WeightChange, 0)
	if len(pkts) > 0 {
		seenChange := make(map[WeightChange]bool)
		for i := 0; i < 6; i++ {
			change := WeightChange{
				At:     rng.Int63n(at + 10),
				Stream: streams[rng.Intn(ns)].ID,
				Weight: int64(1 + rng.Intn(10)),
			}
			key := WeightChange{At: change.At, Stream: change.Stream}
			if seenChange[key] {
				continue
			}
			seenChange[key] = true
			changes = append(changes, change)
		}
		sort.Slice(changes, func(i, j int) bool {
			if changes[i].At != changes[j].At {
				return changes[i].At < changes[j].At
			}
			return changes[i].Stream < changes[j].Stream
		})
	}
	return &Input{Streams: streams, Packets: pkts, WeightChanges: changes}
}

// randomInputWide builds larger random problems at the spec limits.
func randomInputWide(rng *rand.Rand) *Input {
	ns := 2 + rng.Intn(19) // 2..20 streams
	streams := make([]Stream, ns)
	for i := range streams {
		streams[i] = Stream{ID: fmt.Sprintf("s%d", i), Weight: int64(1 + rng.Intn(10))}
	}
	pkts := make([]PacketIn, rng.Intn(501))
	var at int64
	for i := range pkts {
		if rng.Intn(20) == 0 {
			at += int64(rng.Intn(2000))
		}
		at += int64(rng.Intn(3))
		pkts[i] = PacketIn{
			ID:       fmt.Sprintf("p%d", i),
			Stream:   streams[rng.Intn(ns)].ID,
			Arrival:  at,
			Duration: 1 + int64(rng.Intn(1000)),
		}
	}
	changes := make([]WeightChange, 0)
	if len(pkts) > 0 {
		seenChange := make(map[WeightChange]bool)
		for i := 0; i < 20; i++ {
			change := WeightChange{
				At:     rng.Int63n(at + 2000),
				Stream: streams[rng.Intn(ns)].ID,
				Weight: int64(1 + rng.Intn(10)),
			}
			key := WeightChange{At: change.At, Stream: change.Stream}
			if seenChange[key] {
				continue
			}
			seenChange[key] = true
			changes = append(changes, change)
		}
		sort.Slice(changes, func(i, j int) bool {
			if changes[i].At != changes[j].At {
				return changes[i].At < changes[j].At
			}
			return changes[i].Stream < changes[j].Stream
		})
	}
	return &Input{Streams: streams, Packets: pkts, WeightChanges: changes}
}

// TestDifferential cross-checks schedule against the reference simulator
// on random inputs, and validates spec invariants on every result.
func TestDifferential(t *testing.T) {
	rng := rand.New(rand.NewSource(1))
	for iter := 0; iter < 3000; iter++ {
		in := randomInput(rng)
		if err := validate(in); err != nil {
			t.Fatalf("iter %d: generated input invalid: %v", iter, err)
		}
		got := schedule(in)
		if ref := referenceSchedule(in); !reflect.DeepEqual(got, ref) {
			bj, _ := json.Marshal(in)
			gj, _ := json.MarshalIndent(got, "", "  ")
			rj, _ := json.MarshalIndent(ref, "", "  ")
			t.Fatalf("iter %d mismatch\ninput: %s\ngot: %s\nref: %s", iter, bj, gj, rj)
		}
		checkOutput(t, in, got)
	}
}

// TestDifferentialWide exercises the spec limits (20 streams, 500 packets).
func TestDifferentialWide(t *testing.T) {
	rng := rand.New(rand.NewSource(2))
	for iter := 0; iter < 200; iter++ {
		in := randomInputWide(rng)
		if err := validate(in); err != nil {
			t.Fatalf("iter %d: generated input invalid: %v", iter, err)
		}
		got := schedule(in)
		if ref := referenceSchedule(in); !reflect.DeepEqual(got, ref) {
			bj, _ := json.Marshal(in)
			t.Fatalf("iter %d mismatch\ninput: %s", iter, bj)
		}
		checkOutput(t, in, got)
	}
}

func TestValidate(t *testing.T) {
	good := Input{
		Streams: []Stream{{ID: "a", Weight: 1}, {ID: "b", Weight: 10}},
		Packets: []PacketIn{{ID: "p1", Stream: "a", Arrival: 0, Duration: 1}},
	}
	if err := validate(&good); err != nil {
		t.Fatalf("valid input rejected: %v", err)
	}
	empty := Input{Streams: []Stream{{ID: "a", Weight: 1}, {ID: "b", Weight: 1}}}
	if err := validate(&empty); err != nil {
		t.Fatalf("zero packets should be valid: %v", err)
	}

	mk := func(mut func(*Input)) *Input {
		in := Input{
			Streams: []Stream{{ID: "a", Weight: 1}, {ID: "b", Weight: 2}},
			Packets: []PacketIn{
				{ID: "p1", Stream: "a", Arrival: 0, Duration: 1},
				{ID: "p2", Stream: "b", Arrival: 1, Duration: 2},
			},
		}
		mut(&in)
		return &in
	}
	cases := map[string]*Input{
		"one stream":         mk(func(in *Input) { in.Streams = in.Streams[:1] }),
		"twenty one streams": mk(func(in *Input) { in.Streams = append(in.Streams, streamRange(20)...) }),
		"duplicate stream":   mk(func(in *Input) { in.Streams[1].ID = "a" }),
		"empty stream id":    mk(func(in *Input) { in.Streams[0].ID = "" }),
		"space in stream id": mk(func(in *Input) { in.Streams[0].ID = "a b" }),
		"non-ascii stream":   mk(func(in *Input) { in.Streams[0].ID = "é" }),
		"weight zero":        mk(func(in *Input) { in.Streams[0].Weight = 0 }),
		"weight eleven":      mk(func(in *Input) { in.Streams[0].Weight = 11 }),
		"too many packets":   mk(func(in *Input) { in.Packets = packetRange(501) }),
		"duplicate packet":   mk(func(in *Input) { in.Packets[1].ID = "p1" }),
		"unknown stream":     mk(func(in *Input) { in.Packets[0].Stream = "z" }),
		"negative arrival":   mk(func(in *Input) { in.Packets[0].Arrival = -1 }),
		"decreasing arrival": mk(func(in *Input) { in.Packets[1].Arrival = 0; in.Packets[0].Arrival = 3 }),
		"duration zero":      mk(func(in *Input) { in.Packets[0].Duration = 0 }),
		"duration too large": mk(func(in *Input) { in.Packets[0].Duration = 1001 }),
		"empty packet id":    mk(func(in *Input) { in.Packets[0].ID = "" }),
		"unsorted change": mk(func(in *Input) {
			in.WeightChanges = []WeightChange{
				{At: 3, Stream: "a", Weight: 2},
				{At: 2, Stream: "b", Weight: 2},
			}
		}),
		"duplicate same-stream change": mk(func(in *Input) {
			in.WeightChanges = []WeightChange{
				{At: 2, Stream: "a", Weight: 2},
				{At: 2, Stream: "a", Weight: 3},
			}
		}),
	}
	for name, in := range cases {
		if err := validate(in); err == nil {
			t.Errorf("%s: want error, got nil", name)
		}
	}
}

// streamRange returns n extra unique streams with valid weights.
func streamRange(n int) []Stream {
	ss := make([]Stream, n)
	for i := range ss {
		ss[i] = Stream{ID: fmt.Sprintf("x%d", i), Weight: 5}
	}
	return ss
}

// packetRange returns n valid packets on stream "a" with unique ids.
func packetRange(n int) []PacketIn {
	ps := make([]PacketIn, n)
	for i := range ps {
		ps[i] = PacketIn{ID: fmt.Sprintf("q%d", i), Stream: "a", Arrival: int64(i), Duration: 1}
	}
	return ps
}
