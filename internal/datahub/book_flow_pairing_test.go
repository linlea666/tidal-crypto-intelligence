package datahub

import (
	"strconv"
	"testing"
	"time"
)

func TestBookRelativeFormalRequiresTwoRealMatches(t *testing.T) {
	now := time.Date(2026, 10, 14, 3, 0, 0, 0, time.UTC)
	events := []PriceEpisode{{ID: "sell", Direction: "sell", Start: now}, {ID: "buy", Direction: "buy", Start: now}}
	observed := Signal{ID: "observation", Direction: "buy", At: now.Add(-30 * time.Minute)}
	formal := Signal{ID: "formal", Direction: "buy", At: now.Add(30 * time.Minute)}
	for _, tc := range []struct {
		name        string
		observed    []Signal
		formal      []Signal
		pairs       int
		leadMinutes float64
	}{
		{name: "both missing"},
		{name: "observation only", observed: []Signal{observed}},
		{name: "formal only", formal: []Signal{formal}},
		{name: "two sided plus unmatched event", observed: []Signal{observed}, formal: []Signal{formal}, pairs: 1, leadMinutes: 60},
		{name: "different events are not a pair", observed: []Signal{observed}, formal: []Signal{{ID: "sell-formal", Direction: "sell", At: formal.At}}},
		{name: "source IDs do not overwrite times", observed: []Signal{observed}, formal: []Signal{{ID: observed.ID, Direction: formal.Direction, At: formal.At}}, pairs: 1, leadMinutes: 60},
		{name: "missing identity", observed: []Signal{{Direction: observed.Direction, At: observed.At}}, formal: []Signal{formal}},
		{name: "unknown formal time", observed: []Signal{observed}, formal: []Signal{{ID: formal.ID, Direction: formal.Direction}}},
		{name: "following observation has negative lead", observed: []Signal{{ID: observed.ID, Direction: observed.Direction, At: now.Add(90 * time.Minute)}}, formal: []Signal{formal}, pairs: 1, leadMinutes: -60},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := bookRelativeFormal(events, tc.observed, tc.formal, now.Add(8*time.Hour)).(map[string]any)
			pairs := r["pairs"].([]any)
			median := r["medianLeadMinutes"].(*string)
			if r["matchedPairs"] != tc.pairs || len(pairs) != tc.pairs {
				t.Fatalf("unmatched episode became a pair: %#v", r)
			}
			if tc.pairs == 0 {
				if median != nil {
					t.Fatalf("unknown pair became a measured lead: %#v", r)
				}
				return
			}
			if median == nil {
				t.Fatal("missing median")
			}
			v, err := strconv.ParseFloat(*median, 64)
			if err != nil || v != tc.leadMinutes {
				t.Fatalf("median=%v err=%v", median, err)
			}
			p := pairs[0].(map[string]any)
			if p["eventId"] != "buy" || p["observationId"] == "" || p["formalId"] == "" {
				t.Fatalf("pair lost its evidence identities: %#v", p)
			}
		})
	}
}
