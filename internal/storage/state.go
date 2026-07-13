package storage

import (
	"encoding/json"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/IEEECS-VIT/p2p-model-distribution/internal/filemeta"
)

// StateStore manages the download state of a file in a thread-safe manner
// and handles its persistence to state.json.
type StateStore struct {
	mu       sync.RWMutex
	filePath string
	state    *filemeta.DownloadState
}

// NewStateStore creates or loads a StateStore for a given fileID at the specified filePath.
func NewStateStore(filePath string, fileID string) (*StateStore, error) {
	ss := &StateStore{
		filePath: filePath,
	}

	if _, err := os.Stat(filePath); err == nil {
		// File exists, load existing state
		data, err := os.ReadFile(filePath)
		if err != nil {
			return nil, fmt.Errorf("read state file: %w", err)
		}
		var state filemeta.DownloadState
		if err := json.Unmarshal(data, &state); err != nil {
			return nil, fmt.Errorf("unmarshal state: %w", err)
		}
		// Ensure maps are initialized
		if state.Downloaded == nil {
			state.Downloaded = make(map[string]bool)
		}
		if state.Verified == nil {
			state.Verified = make(map[string]bool)
		}
		ss.state = &state
	} else {
		// Initialize a new state
		ss.state = &filemeta.DownloadState{
			FileID:     fileID,
			Status:     "downloading",
			Downloaded: make(map[string]bool),
			Verified:   make(map[string]bool),
			UpdatedAt:  time.Now().Unix(),
		}
		// Save initial state to disk
		if err := ss.save(); err != nil {
			return nil, err
		}
	}

	return ss, nil
}

// MarkChunkDownloaded marks a chunk key as downloaded and saves the updated state.
func (ss *StateStore) MarkChunkDownloaded(chunkKey string) error {
	ss.mu.Lock()
	defer ss.mu.Unlock()

	ss.state.Downloaded[chunkKey] = true
	ss.state.UpdatedAt = time.Now().Unix()

	return ss.save()
}

// MarkChunkVerified marks a chunk key as verified and saves the updated state.
func (ss *StateStore) MarkChunkVerified(chunkKey string) error {
	ss.mu.Lock()
	defer ss.mu.Unlock()

	ss.state.Verified[chunkKey] = true
	ss.state.UpdatedAt = time.Now().Unix()

	return ss.save()
}

// UpdateStatus updates the overall download status (e.g. "completed", "failed") and saves state.
func (ss *StateStore) UpdateStatus(status string) error {
	ss.mu.Lock()
	defer ss.mu.Unlock()

	ss.state.Status = status
	ss.state.UpdatedAt = time.Now().Unix()

	return ss.save()
}

// GetState returns a deep copy of the current download state to avoid race conditions on maps.
func (ss *StateStore) GetState() filemeta.DownloadState {
	ss.mu.RLock()
	defer ss.mu.RUnlock()

	copiedDownloaded := make(map[string]bool, len(ss.state.Downloaded))
	for k, v := range ss.state.Downloaded {
		copiedDownloaded[k] = v
	}
	copiedVerified := make(map[string]bool, len(ss.state.Verified))
	for k, v := range ss.state.Verified {
		copiedVerified[k] = v
	}

	return filemeta.DownloadState{
		FileID:     ss.state.FileID,
		Status:     ss.state.Status,
		Downloaded: copiedDownloaded,
		Verified:   copiedVerified,
		UpdatedAt:  ss.state.UpdatedAt,
	}
}

// save serializes the current state to the config file path.
// Callers of this helper must hold the write lock.
func (ss *StateStore) save() error {
	data, err := json.MarshalIndent(ss.state, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal state: %w", err)
	}

	tmpPath := ss.filePath + ".tmp"
	f, err := os.OpenFile(tmpPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		return fmt.Errorf("open tmp state file: %w", err)
	}
	defer func() {
		_ = f.Close()
		_ = os.Remove(tmpPath)
	}()

	if _, err := f.Write(data); err != nil {
		return fmt.Errorf("write tmp state file: %w", err)
	}

	if err := f.Sync(); err != nil {
		return fmt.Errorf("sync tmp state file: %w", err)
	}

	if err := f.Close(); err != nil {
		return fmt.Errorf("close tmp state file: %w", err)
	}

	if err := os.Rename(tmpPath, ss.filePath); err != nil {
		return fmt.Errorf("rename state file: %w", err)
	}

	return nil
}
