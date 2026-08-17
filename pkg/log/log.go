package log

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/txltedxgod/nexus-mq/pkg/proto"
)

// Log represents a partitioned commit log consisting of multiple segments.
type Log struct {
	mu             sync.RWMutex
	dir            string
	maxSegmentBytes uint64
	retentionAge   time.Duration
	segments       []*Segment
	activeSegment  *Segment
	closed         bool
}

// Config defines options for Log.
type Config struct {
	Directory       string
	MaxSegmentBytes uint64
	RetentionAge    time.Duration
}

// NewLog initializes or recovers a partitioned log directory.
func NewLog(cfg Config) (*Log, error) {
	if cfg.MaxSegmentBytes == 0 {
		cfg.MaxSegmentBytes = 10 * 1024 * 1024 // 10 MB default segment
	}
	if cfg.RetentionAge == 0 {
		cfg.RetentionAge = 7 * 24 * time.Hour
	}

	if err := os.MkdirAll(cfg.Directory, 0755); err != nil {
		return nil, fmt.Errorf("failed to create log dir: %w", err)
	}

	l := &Log{
		dir:             cfg.Directory,
		maxSegmentBytes: cfg.MaxSegmentBytes,
		retentionAge:   cfg.RetentionAge,
	}

	if err := l.recover(); err != nil {
		return nil, fmt.Errorf("failed to recover log: %w", err)
	}

	return l, nil
}

func (l *Log) recover() error {
	entries, err := os.ReadDir(l.dir)
	if err != nil {
		return err
	}

	var baseOffsets []uint64
	for _, entry := range entries {
		if !entry.IsDir() && strings.HasSuffix(entry.Name(), ".log") {
			name := strings.TrimSuffix(entry.Name(), ".log")
			offset, err := strconv.ParseUint(name, 10, 64)
			if err == nil {
				baseOffsets = append(baseOffsets, offset)
			}
		}
	}

	sort.Slice(baseOffsets, func(i, j int) bool {
		return baseOffsets[i] < baseOffsets[j]
	})

	if len(baseOffsets) == 0 {
		seg, err := NewSegment(l.dir, 0, l.maxSegmentBytes)
		if err != nil {
			return err
		}
		l.segments = []*Segment{seg}
		l.activeSegment = seg
		return nil
	}

	for _, offset := range baseOffsets {
		seg, err := NewSegment(l.dir, offset, l.maxSegmentBytes)
		if err != nil {
			return err
		}
		l.segments = append(l.segments, seg)
	}

	l.activeSegment = l.segments[len(l.segments)-1]
	return nil
}

// Append writes a message into the active segment, rolling over if necessary.
func (l *Log) Append(msg *proto.Message) (uint64, error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	if l.closed {
		return 0, fmt.Errorf("log is closed")
	}

	if l.activeSegment.IsFull() {
		newOffset := l.activeSegment.nextOffset
		newSeg, err := NewSegment(l.dir, newOffset, l.maxSegmentBytes)
		if err != nil {
			return 0, fmt.Errorf("failed to roll segment: %w", err)
		}
		l.segments = append(l.segments, newSeg)
		l.activeSegment = newSeg
	}

	return l.activeSegment.Append(msg)
}

// Read reads a message at the specified offset.
func (l *Log) Read(offset uint64) (*proto.Message, error) {
	l.mu.RLock()
	defer l.mu.RUnlock()

	if l.closed {
		return nil, fmt.Errorf("log is closed")
	}

	seg := l.findSegment(offset)
	if seg == nil {
		return nil, fmt.Errorf("offset %d not found in log", offset)
	}

	return seg.ReadAt(offset)
}

// ReadBatch reads up to limit messages starting at startOffset.
func (l *Log) ReadBatch(startOffset uint64, limit int) ([]*proto.Message, uint64, error) {
	l.mu.RLock()
	defer l.mu.RUnlock()

	if l.closed {
		return nil, 0, fmt.Errorf("log is closed")
	}

	if limit <= 0 {
		limit = 100
	}

	var results []*proto.Message
	current := startOffset
	latest := l.activeSegment.nextOffset

	for current < latest && len(results) < limit {
		seg := l.findSegment(current)
		if seg == nil {
			break
		}
		msg, err := seg.ReadAt(current)
		if err != nil {
			break
		}
		results = append(results, msg)
		current++
	}

	return results, current, nil
}

func (l *Log) findSegment(offset uint64) *Segment {
	// Binary search across segments sorted by baseOffset
	idx := sort.Search(len(l.segments), func(i int) bool {
		return l.segments[i].baseOffset > offset
	})

	if idx > 0 {
		candidate := l.segments[idx-1]
		if offset >= candidate.baseOffset && offset < candidate.nextOffset {
			return candidate
		}
	}
	return nil
}

// LatestOffset returns the next offset to be written.
func (l *Log) LatestOffset() uint64 {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.activeSegment.nextOffset
}

// OldestOffset returns the earliest available offset.
func (l *Log) OldestOffset() uint64 {
	l.mu.RLock()
	defer l.mu.RUnlock()
	if len(l.segments) == 0 {
		return 0
	}
	return l.segments[0].baseOffset
}

// TotalSegments returns number of open segments.
func (l *Log) TotalSegments() int {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return len(l.segments)
}

// Sync flushes all segments to disk.
func (l *Log) Sync() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.activeSegment.Sync()
}

// Close gracefully closes all segment files.
func (l *Log) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.closed = true
	var firstErr error
	for _, seg := range l.segments {
		if err := seg.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}
