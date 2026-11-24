/*
 * MinIO Cloud Storage, (C) 2020 MinIO, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package simdjson

import (
	"fmt"
	"sort"
	"strconv"
	"strings"
)

// PathSegment is a single segment of an allowlist path.
// Exactly one of Key (object member) or Index (array element) applies:
// when IsIndex is true the segment addresses an array element, otherwise
// it addresses an object member stored in Key.
type PathSegment struct {
	// Key is the object member name. Only valid when IsIndex is false.
	Key string

	// Index is the zero-based array element index. Only valid when IsIndex is true.
	Index uint64

	// IsIndex reports whether this segment addresses an array element.
	IsIndex bool
}

// PathIssueKind classifies a PathIssue.
type PathIssueKind uint8

const (
	// IssueInvalid is reported for paths that are syntactically invalid,
	// cannot be located in the data, hit a type mismatch or run past the
	// end of a scalar value.
	IssueInvalid PathIssueKind = iota

	// IssueDuplicate is reported when the exact same path is listed more
	// than once in the configuration.
	IssueDuplicate
)

// String returns a human readable representation of the kind.
func (k PathIssueKind) String() string {
	switch k {
	case IssueInvalid:
		return "invalid path"
	case IssueDuplicate:
		return "duplicate path"
	default:
		return "unknown issue"
	}
}

// PathIssue describes a problem with an allowlist path.
//
// Index is the zero-based position of the offending entry within the
// configuration slice passed to ParseAllowlistPaths, so the entry can be
// located in the configuration. Path is the entry as it was provided.
// Segment is the zero-based index of the path segment where the problem
// was detected, or -1 when the problem applies to the whole path
// (syntax errors and duplicates).
type PathIssue struct {
	// Index is the zero-based index of the path within the input configuration.
	Index int

	// Path is the raw path as provided in the configuration.
	Path string

	// Kind classifies the issue.
	Kind PathIssueKind

	// Segment is the zero-based index of the failing path segment,
	// or -1 when the issue applies to the whole path.
	Segment int

	// Message describes the problem.
	Message string
}

// Error implements the error interface.
func (p PathIssue) Error() string {
	if p.Segment < 0 {
		return fmt.Sprintf("allowlist path %d %q: %s: %s", p.Index, p.Path, p.Kind, p.Message)
	}
	return fmt.Sprintf("allowlist path %d %q: %s at segment %d: %s", p.Index, p.Path, p.Kind, p.Segment, p.Message)
}

// pathNode is a trie node matching parsed path segments against the JSON tape.
type pathNode struct {
	// keyChildren matches object member segments.
	keyChildren map[string]*pathNode

	// indexChildren matches array index segments.
	indexChildren map[uint64]*pathNode

	// terminal holds the configuration indices whose paths end at this node.
	// More than one entry means the path was listed multiple times.
	terminal []int

	// depth is the number of segments consumed to reach this node.
	depth int

	// matched is set during Project when this node is covered by output.
	matched bool

	// reported is set during Project when its terminals already have an issue.
	reported bool
}

// PathAllowlist holds the set of explicitly approved paths that are allowed
// to appear in projected output.
//
// A PathAllowlist is safe to reuse across calls, but a single instance must
// not be used concurrently: Project resets per-call match state.
type PathAllowlist struct {
	root *pathNode

	// raw keeps the original configuration entries, indexed by configuration index.
	raw []string

	// configIssues holds build-time issues (syntax errors and duplicates).
	configIssues []PathIssue
}

// ParseAllowlistPath parses a single path.
//
// Paths are slash separated segments, for example "counterparty/name".
// A segment consisting solely of decimal digits addresses a zero-based
// array element, e.g. "items/0/sku"; every other segment addresses an
// object member. Empty paths, empty segments (leading/trailing or repeated
// slashes) and array indices that do not fit in uint64 are rejected.
func ParseAllowlistPath(p string) ([]PathSegment, error) {
	if p == "" {
		return nil, fmt.Errorf("path is empty")
	}
	parts := strings.Split(p, "/")
	segs := make([]PathSegment, 0, len(parts))
	for i, part := range parts {
		if part == "" {
			return nil, fmt.Errorf("segment %d is empty", i)
		}
		if isAllDigits(part) {
			idx, err := strconv.ParseUint(part, 10, 64)
			if err != nil {
				return nil, fmt.Errorf("segment %d has array index %q out of range: %w", i, part, err)
			}
			segs = append(segs, PathSegment{Index: idx, IsIndex: true})
			continue
		}
		segs = append(segs, PathSegment{Key: part})
	}
	return segs, nil
}

func isAllDigits(s string) bool {
	for i := 0; i < len(s); i++ {
		if s[i] < '0' || s[i] > '9' {
			return false
		}
	}
	return len(s) > 0
}

// ParseAllowlistPaths builds a PathAllowlist from raw path strings.
//
// Every configuration entry keeps its position so returned issues can point
// back at it via PathIssue.Index. Syntactically invalid entries and exact
// duplicates are returned as issues and never take part in projection.
// A non-nil allowlist is always returned, even when issues are present, so
// valid entries can still be inspected or used.
func ParseAllowlistPaths(paths []string) (*PathAllowlist, []PathIssue) {
	p := &PathAllowlist{
		root: &pathNode{},
		raw:  append([]string(nil), paths...),
	}
	for idx, raw := range paths {
		segs, err := ParseAllowlistPath(raw)
		if err != nil {
			p.configIssues = append(p.configIssues, PathIssue{
				Index:   idx,
				Path:    raw,
				Kind:    IssueInvalid,
				Segment: -1,
				Message: err.Error(),
			})
			continue
		}
		p.insert(idx, raw, segs)
	}
	return p, append([]PathIssue(nil), p.configIssues...)
}

// insert adds a syntactically valid path to the trie and records duplicates.
func (p *PathAllowlist) insert(idx int, raw string, segs []PathSegment) {
	node := p.root
	for _, seg := range segs {
		if seg.IsIndex {
			if node.indexChildren == nil {
				node.indexChildren = make(map[uint64]*pathNode)
			}
			child, ok := node.indexChildren[seg.Index]
			if !ok {
				child = &pathNode{depth: node.depth + 1}
				node.indexChildren[seg.Index] = child
			}
			node = child
			continue
		}
		if node.keyChildren == nil {
			node.keyChildren = make(map[string]*pathNode)
		}
		child, ok := node.keyChildren[seg.Key]
		if !ok {
			child = &pathNode{depth: node.depth + 1}
			node.keyChildren[seg.Key] = child
		}
		node = child
	}
	if len(node.terminal) > 0 {
		p.configIssues = append(p.configIssues, PathIssue{
			Index:   idx,
			Path:    raw,
			Kind:    IssueDuplicate,
			Segment: -1,
			Message: fmt.Sprintf("path already listed at index %d", node.terminal[0]),
		})
		return
	}
	node.terminal = append(node.terminal, idx)
}

// Project generates new JSON output containing only the values reachable
// through the allowlist paths of the parsed JSON.
//
// The parsed JSON is read only: its tape, message and string buffer are not
// modified and can be kept as the untouched reference record. Output is built
// from scratch, preserving object member relationships, member order and
// array element order. Sibling members and array elements that no approved
// path reaches are omitted.
//
// For NDJSON input every input record produces one output line, in order.
// Data-level problems (missing paths, object/array type mismatches, array
// indices out of range, paths continuing past scalar values) are returned as
// issues, located by configuration index and failing segment. A projection is
// still produced from whatever matched; err is non-nil only when the tape
// itself cannot be traversed.
func (p *PathAllowlist) Project(pj *ParsedJson) (output []byte, issues []PathIssue, err error) {
	if pj == nil {
		return nil, nil, fmt.Errorf("cannot project a nil ParsedJson")
	}

	// Build-time problems apply regardless of the data.
	issues = append(issues, p.configIssues...)
	dst := make([]byte, 0, len(pj.Message))

	// Each root tag is one JSON document (NDJSON may contain several).
	i := pj.Iter()
	var root Iter
	rootCount := 0
	for {
		t, err := i.AdvanceIter(&root)
		if err != nil {
			return nil, p.dedupeIssues(issues), err
		}
		if t == TypeNone {
			break
		}
		if t != TypeRoot {
			return nil, p.dedupeIssues(issues), fmt.Errorf("expected root element, got %v", t)
		}
		// Match state is per record: a path failing in one record must be
		// reported even if the same path matched an earlier record.
		p.resetMatchState()
		if rootCount > 0 {
			dst = append(dst, '\n')
		}
		rootCount++
		root.AdvanceInto()
		var emitted bool
		dst, emitted, err = p.emitValue(root, p.root, dst, &issues)
		if err != nil {
			return nil, p.dedupeIssues(issues), err
		}
		if !emitted {
			// A scalar root with no matching path: keep the output line
			// valid JSON instead of emitting an empty document.
			dst = append(dst, "null"...)
		}
		p.reportUnmatched(p.root, &issues)
	}
	if rootCount == 0 {
		return nil, p.dedupeIssues(issues), fmt.Errorf("no JSON document found in parsed input")
	}

	// The same configuration path may fail in several records; keep the
	// first report per configuration index.
	issues = p.dedupeIssues(issues)
	sort.SliceStable(issues, func(a, b int) bool { return issues[a].Index < issues[b].Index })
	return dst, issues, nil
}

// dedupeIssues keeps the first issue per configuration index, preserving order.
func (p *PathAllowlist) dedupeIssues(in []PathIssue) []PathIssue {
	seen := make(map[int]struct{}, len(in))
	out := in[:0]
	for _, is := range in {
		if _, ok := seen[is.Index]; ok {
			continue
		}
		seen[is.Index] = struct{}{}
		out = append(out, is)
	}
	return out
}

// resetMatchState clears per-projection flags on the trie.
func (p *PathAllowlist) resetMatchState() {
	var walk func(n *pathNode)
	walk = func(n *pathNode) {
		n.matched = false
		n.reported = false
		for _, c := range n.keyChildren {
			walk(c)
		}
		for _, c := range n.indexChildren {
			walk(c)
		}
	}
	walk(p.root)
}

// emitValue writes the value queued in i, restricted to paths below node.
// It returns the extended buffer and whether anything was written.
func (p *PathAllowlist) emitValue(i Iter, node *pathNode, dst []byte, issues *[]PathIssue) ([]byte, bool, error) {
	// A path ends here: the whole value is approved, including every
	// descendant member or element.
	if len(node.terminal) > 0 {
		p.markSubtreeMatched(node)
		var err error
		dst, err = i.MarshalJSONBuffer(dst)
		if err != nil {
			return dst, false, err
		}
		return dst, true, nil
	}

	switch i.t {
	case TagObjectStart:
		// Index segments cannot match object members.
		for _, child := range node.indexChildren {
			p.reportSubtree(child, issues, child.depth-1,
				fmt.Sprintf("segment expects an array index but the value is an object"))
		}
		var o Object
		obj, err := i.Object(&o)
		if err != nil {
			return dst, false, err
		}
		first := true
		var val Iter
		matchedKeys := make(map[string]struct{}, len(node.keyChildren))
		for {
			name, t, err := obj.NextElementBytes(&val)
			if err != nil {
				return dst, false, err
			}
			if t == TypeNone {
				break
			}
			child := p.lookupKeyChild(node, name)
			if child == nil {
				continue
			}
			matchedKeys[string(name)] = struct{}{}
			before := len(dst)
			if first {
				dst = append(dst, '{')
			} else {
				dst = append(dst, ',')
			}
			dst = append(dst, '"')
			dst = escapeBytes(dst, name)
			dst = append(dst, '"', ':')
			emitted := false
			dst, emitted, err = p.emitValue(val, child, dst, issues)
			if err != nil {
				return dst, false, err
			}
			if !emitted {
				// Roll back the key of a subtree that produced no value.
				dst = dst[:before]
				continue
			}
			first = false
		}
		// Approved member names that were never present report at this
		// segment rather than at an overshoot segment.
		for key, child := range node.keyChildren {
			if _, ok := matchedKeys[key]; !ok {
				p.reportSubtree(child, issues, child.depth-1,
					fmt.Sprintf("object has no member %q", key))
			}
		}
		if first {
			// Nothing approved was emitted: omit this object entirely.
			// A scalar root with no match is handled by Project.
			return dst, false, nil
		}
		dst = append(dst, '}')
		return dst, true, nil

	case TagArrayStart:
		// Member-name segments cannot match array elements.
		for _, child := range node.keyChildren {
			p.reportSubtree(child, issues, child.depth-1,
				fmt.Sprintf("segment expects an object member but the value is an array"))
		}
		var a Array
		arr, err := i.Array(&a)
		if err != nil {
			return dst, false, err
		}
		// AdvanceIter gives each element its own correctly bounded tape,
		// matching how Array.MarshalJSONBuffer traverses arrays: the array's
		// own tape slice excludes the closing TagArrayEnd.
		ai := arr.Iter()
		var elem Iter
		first := true
		idx := 0
		for {
			t, err := ai.AdvanceIter(&elem)
			if err != nil {
				return dst, false, err
			}
			if t == TypeNone {
				break
			}
			child := node.indexChildren[uint64(idx)]
			if child != nil {
				before := len(dst)
				if first {
					dst = append(dst, '[')
				} else {
					dst = append(dst, ',')
				}
				emitted := false
				dst, emitted, err = p.emitValue(elem, child, dst, issues)
				if err != nil {
					return dst, false, err
				}
				if !emitted {
					dst = dst[:before]
				} else {
					first = false
				}
			}
			idx++
		}
		if first {
			// Nothing approved was emitted: omit this array entirely.
			// A scalar root with no match is handled by Project.
			return dst, false, nil
		}
		dst = append(dst, ']')
		// Report approved indices beyond the end of the array.
		for arrIdx, child := range node.indexChildren {
			if int(arrIdx) >= idx {
				p.reportSubtree(child, issues, child.depth-1,
					fmt.Sprintf("array index %d is out of range, array has %d element(s)", arrIdx, idx))
			}
		}
		return dst, true, nil

	default:
		// Scalar value while the path still expects more segments.
		if !isScalarTag(i.t) {
			return dst, false, fmt.Errorf("unexpected tape tag %q while projecting value", i.t.String())
		}
		p.reportSubtree(node, issues, -1,
			fmt.Sprintf("path continues beyond a scalar value of type %v", i.t.Type()))
		return dst, false, nil
	}
}

// lookupKeyChild finds the trie child for an object member name.
func (p *PathAllowlist) lookupKeyChild(node *pathNode, name []byte) *pathNode {
	if node.keyChildren == nil {
		return nil
	}
	return node.keyChildren[string(name)]
}

// isScalarTag reports whether the tag is a JSON scalar.
func isScalarTag(t Tag) bool {
	switch t {
	case TagString, TagInteger, TagUint, TagFloat, TagNull, TagBoolTrue, TagBoolFalse:
		return true
	}
	return false
}

// markSubtreeMatched marks every terminal below node as covered by output.
func (p *PathAllowlist) markSubtreeMatched(node *pathNode) {
	var walk func(n *pathNode)
	walk = func(n *pathNode) {
		n.matched = true
		for _, c := range n.keyChildren {
			walk(c)
		}
		for _, c := range n.indexChildren {
			walk(c)
		}
	}
	walk(node)
}

// reportSubtree emits one issue per configuration path ending below node,
// unless that part of the trie has already been reported or matched.
//
// When segAt is >= 0 every issue points at that segment, used for structural
// divergences where traversal leaves the data at the entry node. A negative
// segAt reports each terminal at its own final segment, used for paths that
// continue past a scalar value.
func (p *PathAllowlist) reportSubtree(node *pathNode, issues *[]PathIssue, segAt int, msg string) {
	var walk func(n *pathNode)
	walk = func(n *pathNode) {
		if n.reported || n.matched {
			return
		}
		n.reported = true
		seg := segAt
		if seg < 0 {
			seg = n.depth - 1
		}
		for _, cfgIdx := range n.terminal {
			*issues = append(*issues, PathIssue{
				Index:   cfgIdx,
				Path:    p.raw[cfgIdx],
				Kind:    IssueInvalid,
				Segment: seg,
				Message: msg,
			})
		}
		for _, c := range n.keyChildren {
			walk(c)
		}
		for _, c := range n.indexChildren {
			walk(c)
		}
	}
	walk(node)
}

// reportUnmatched reports terminals that no part of the data covered.
func (p *PathAllowlist) reportUnmatched(node *pathNode, issues *[]PathIssue) {
	var walk func(n *pathNode)
	walk = func(n *pathNode) {
		if n.reported || n.matched {
			return
		}
		for _, cfgIdx := range n.terminal {
			*issues = append(*issues, PathIssue{
				Index:   cfgIdx,
				Path:    p.raw[cfgIdx],
				Kind:    IssueInvalid,
				Segment: n.depth - 1,
				Message: "path not found in JSON",
			})
		}
		n.reported = true
		for _, c := range n.keyChildren {
			walk(c)
		}
		for _, c := range n.indexChildren {
			walk(c)
		}
	}
	walk(node)
}
