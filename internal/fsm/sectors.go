package fsm

// Sector map: which part of the airspace each sector controls.
//
// It lives in the same replicated state machine as the ownership table, and is
// only used on the AIRSPACE REGISTRY cluster. Keeping both in one log is what
// makes elastic sectors safe: a sector cannot be merged away while the registry
// still says it owns a flight, because both facts are checked in one Apply.
//
// Ops (all committed through Raft like every other command):
//
//	sector_init  Key ""            Value []Sector     seed the map once at start-up
//	split        Key parent        Value SplitReq     parent keeps one half, a STANDBY sector takes the other
//	drain        Key sector        Value ignored      ACTIVE -> DRAINING (takes no new flights)
//	undrain      Key sector        Value ignored      DRAINING -> ACTIVE (cancel a merge)
//	merge        Key drained       Value MergeReq     fold a DRAINING sector into a neighbour, back to STANDBY

import (
	"encoding/json"
	"fmt"
	"sort"

	"github.com/hashicorp/raft"
)

// Sector statuses. These describe the sector map, not flight ownership (that is
// Active / Transferring in fsm.go).
const (
	SectorActive   = "ACTIVE"   // owns a region and accepts flights
	SectorStandby  = "STANDBY"  // idle cluster with no region, free to be split into
	SectorDraining = "DRAINING" // being merged away: accepts no new flights, hands its flights off
)

// Region is an axis-aligned rectangle of airspace in the simulator's units.
// It is half-open: a point on MinX/MinY belongs to the region, a point on
// MaxX/MaxY belongs to the neighbour. That way every point is in exactly one
// sector, with no double ownership along a border.
type Region struct {
	MinX float64 `json:"minX"`
	MinY float64 `json:"minY"`
	MaxX float64 `json:"maxX"`
	MaxY float64 `json:"maxY"`
}

func (r Region) Contains(x, y float64) bool {
	return x >= r.MinX && x < r.MaxX && y >= r.MinY && y < r.MaxY
}

func (r Region) Empty() bool { return r.MaxX <= r.MinX || r.MaxY <= r.MinY }

func (r Region) Overlaps(o Region) bool {
	return r.MinX < o.MaxX && o.MinX < r.MaxX && r.MinY < o.MaxY && o.MinY < r.MaxY
}

// Union returns the rectangle covering r and o, but only when they share a
// whole edge, so that merging them leaves no gap and no overlap.
func (r Region) Union(o Region) (Region, bool) {
	switch {
	case r.MinY == o.MinY && r.MaxY == o.MaxY && r.MaxX == o.MinX: // o is right of r
		return Region{r.MinX, r.MinY, o.MaxX, r.MaxY}, true
	case r.MinY == o.MinY && r.MaxY == o.MaxY && o.MaxX == r.MinX: // o is left of r
		return Region{o.MinX, r.MinY, r.MaxX, r.MaxY}, true
	case r.MinX == o.MinX && r.MaxX == o.MaxX && r.MaxY == o.MinY: // o is above r
		return Region{r.MinX, r.MinY, r.MaxX, o.MaxY}, true
	case r.MinX == o.MinX && r.MaxX == o.MaxX && o.MaxY == r.MinY: // o is below r
		return Region{r.MinX, o.MinY, r.MaxX, r.MaxY}, true
	}
	return Region{}, false
}

// Sector is one entry in the map.
type Sector struct {
	Name   string `json:"name"`   // the Raft cluster name, e.g. "sector-1"
	Status string `json:"status"` // SectorActive, SectorStandby or SectorDraining
	Region Region `json:"region"` // zero value while STANDBY
	Index  uint64 `json:"index"`  // log index of the last change
}

// SectorMap is a copy of the whole map, safe to hand to other goroutines.
type SectorMap struct {
	Version uint64            `json:"version"` // log index of the last map change; compare to spot a stale copy
	Sectors map[string]Sector `json:"sectors"`
}

// SplitReq is the payload of "split".
type SplitReq struct {
	Child string `json:"child"` // a STANDBY sector that takes the new half
	Axis  string `json:"axis"`  // "x" cuts left|right, "y" cuts bottom|top; empty cuts the longer side
}

// MergeReq is the payload of "merge".
type MergeReq struct {
	Into string `json:"into"` // an ACTIVE neighbour that absorbs the drained sector
}

// Sectors returns a copy of the sector map. Like the other reads, it is local
// and may be stale on a follower; ask the registry leader when it matters.
func (f *FSM) Sectors() SectorMap {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.sectorMapLocked()
}

func (f *FSM) sectorMapLocked() SectorMap {
	out := SectorMap{Version: f.mapVer, Sectors: make(map[string]Sector, len(f.sectors))}
	for k, v := range f.sectors {
		out.Sectors[k] = v
	}
	return out
}

// canReceive rejects new flights for a sector that the map says is not ACTIVE.
// A sector the map does not know (the tower, or a registry that was never
// seeded) is allowed, so the original handoff behaviour is unchanged.
func (f *FSM) canReceive(sector string) error {
	if s, ok := f.sectors[sector]; ok && s.Status != SectorActive {
		return fmt.Errorf("sector %s is %s and cannot take new flights", sector, s.Status)
	}
	return nil
}

// flightsTouching counts flights the registry says this sector holds or is
// about to receive.
func (f *FSM) flightsTouching(sector string) int {
	n := 0
	for _, o := range f.owners {
		if o.Sector == sector || o.Target == sector {
			n++
		}
	}
	return n
}

// applySector runs one sector-map op. The caller (Apply) already holds f.mu.
// It returns an error (committed but rejected, nothing changed) or the new
// SectorMap.
func (f *FSM) applySector(l *raft.Log, c Command) interface{} {
	switch c.Op {

	case "sector_init":
		if len(f.sectors) > 0 {
			return fmt.Errorf("sector map already initialised")
		}
		var in []Sector
		if err := json.Unmarshal(c.Value, &in); err != nil {
			return fmt.Errorf("bad sector_init payload: %w", err)
		}
		next := make(map[string]Sector, len(in))
		for _, s := range in {
			if s.Name == "" {
				return fmt.Errorf("sector_init: sector without a name")
			}
			if _, dup := next[s.Name]; dup {
				return fmt.Errorf("sector_init: duplicate sector %s", s.Name)
			}
			switch s.Status {
			case SectorActive:
				if s.Region.Empty() {
					return fmt.Errorf("sector_init: ACTIVE sector %s has no region", s.Name)
				}
			case SectorStandby:
				s.Region = Region{}
			default:
				return fmt.Errorf("sector_init: %s must start ACTIVE or STANDBY, not %q", s.Name, s.Status)
			}
			s.Index = l.Index
			next[s.Name] = s
		}
		for _, a := range next {
			for _, b := range next {
				if a.Name < b.Name && a.Status == SectorActive && b.Status == SectorActive && a.Region.Overlaps(b.Region) {
					return fmt.Errorf("sector_init: %s and %s overlap", a.Name, b.Name)
				}
			}
		}
		f.sectors = next

	case "split":
		var req SplitReq
		if err := json.Unmarshal(c.Value, &req); err != nil {
			return fmt.Errorf("bad split payload: %w", err)
		}
		parent, ok := f.sectors[c.Key]
		if !ok || parent.Status != SectorActive {
			return fmt.Errorf("cannot split %s: not an ACTIVE sector (%+v)", c.Key, parent)
		}
		child, ok := f.sectors[req.Child]
		if !ok || child.Status != SectorStandby {
			return fmt.Errorf("cannot split into %s: not a STANDBY sector (%+v)", req.Child, child)
		}
		if parent.Region.Empty() {
			return fmt.Errorf("cannot split %s: empty region", c.Key)
		}
		axis := req.Axis
		if axis == "" {
			if parent.Region.MaxX-parent.Region.MinX >= parent.Region.MaxY-parent.Region.MinY {
				axis = "x"
			} else {
				axis = "y"
			}
		}
		lo, hi := parent.Region, parent.Region
		switch axis {
		case "x":
			mid := (parent.Region.MinX + parent.Region.MaxX) / 2
			lo.MaxX, hi.MinX = mid, mid
		case "y":
			mid := (parent.Region.MinY + parent.Region.MaxY) / 2
			lo.MaxY, hi.MinY = mid, mid
		default:
			return fmt.Errorf("split axis must be x or y, not %q", axis)
		}
		parent.Region, parent.Index = lo, l.Index
		child.Region, child.Status, child.Index = hi, SectorActive, l.Index
		f.sectors[parent.Name], f.sectors[child.Name] = parent, child

	case "drain":
		s, ok := f.sectors[c.Key]
		if !ok || s.Status != SectorActive {
			return fmt.Errorf("cannot drain %s: not an ACTIVE sector (%+v)", c.Key, s)
		}
		// Refuse if there is nobody to hand the flights to: the sector could
		// never be merged and would sit DRAINING for ever.
		hasNeighbour := false
		for _, o := range f.sectors {
			if o.Name == s.Name || o.Status != SectorActive {
				continue
			}
			if _, ok := s.Region.Union(o.Region); ok {
				hasNeighbour = true
				break
			}
		}
		if !hasNeighbour {
			return fmt.Errorf("cannot drain %s: no ACTIVE neighbour shares a full edge", c.Key)
		}
		s.Status, s.Index = SectorDraining, l.Index
		f.sectors[s.Name] = s

	case "undrain":
		s, ok := f.sectors[c.Key]
		if !ok || s.Status != SectorDraining {
			return fmt.Errorf("cannot undrain %s: not DRAINING (%+v)", c.Key, s)
		}
		s.Status, s.Index = SectorActive, l.Index
		f.sectors[s.Name] = s

	case "merge":
		var req MergeReq
		if err := json.Unmarshal(c.Value, &req); err != nil {
			return fmt.Errorf("bad merge payload: %w", err)
		}
		child, ok := f.sectors[c.Key]
		if !ok || child.Status != SectorDraining {
			return fmt.Errorf("cannot merge %s: not DRAINING (%+v)", c.Key, child)
		}
		into, ok := f.sectors[req.Into]
		if !ok || into.Status != SectorActive {
			return fmt.Errorf("cannot merge into %s: not an ACTIVE sector (%+v)", req.Into, into)
		}
		union, ok := into.Region.Union(child.Region)
		if !ok {
			return fmt.Errorf("cannot merge %s into %s: regions do not share a full edge", c.Key, req.Into)
		}
		if n := f.flightsTouching(c.Key); n > 0 {
			return fmt.Errorf("cannot merge %s: registry still lists %d flight(s) held by or heading to it", c.Key, n)
		}
		into.Region, into.Index = union, l.Index
		child.Status, child.Region, child.Index = SectorStandby, Region{}, l.Index
		f.sectors[into.Name], f.sectors[child.Name] = into, child

	default:
		return fmt.Errorf("unknown sector op %q", c.Op)
	}

	f.mapVer = l.Index
	return f.sectorMapLocked()
}

// SectorNames returns the sector names in a stable order (handy for logs and tests).
func (m SectorMap) SectorNames() []string {
	names := make([]string, 0, len(m.Sectors))
	for n := range m.Sectors {
		names = append(names, n)
	}
	sort.Strings(names)
	return names
}
