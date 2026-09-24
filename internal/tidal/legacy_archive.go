package tidal

import (
	"encoding/json"
	"time"
)

// MaintainArchive completes only the old source's remaining rollups, then ages
// its partitions independently. It neither starts collectors nor creates frames.
func (s *Store) MaintainArchive(now time.Time) error {
	var cutoff, through int64
	if raw, ok := s.Get("v1ArchiveCutoff"); ok {
		_ = json.Unmarshal([]byte(raw), &cutoff)
	}
	if cutoff == 0 {
		cutoff = now.UTC().Truncate(24 * time.Hour).Add(24 * time.Hour).Unix()
		if err := s.Put("v1ArchiveCutoff", cutoff); err != nil {
			return err
		}
	}
	if raw, ok := s.Get("rollupThrough"); ok {
		_ = json.Unmarshal([]byte(raw), &through)
	}
	for n := 0; n < 8 && through > 0 && through < cutoff; n++ {
		if err := s.Rollup(time.Unix(through, 0)); err != nil {
			return err
		}
		through += 900
	}
	if now.Sub(s.Report().LastCleanup) > time.Hour {
		return s.Cleanup(now)
	}
	return nil
}
