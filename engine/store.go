package engine

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
)

// ErrConflict 表示幂等键冲突：已有不同内容的执行占用该键。
var ErrConflict = errors.New("engine: idempotency key conflict")

// ErrNotFound 表示执行记录不存在。
var ErrNotFound = errors.New("engine: run not found")

// Store 持久化执行记录，支持进程重启后的恢复。
type Store interface {
	// CreateIfAbsent 以幂等键占位。已存在时返回既有记录；
	// 若既有记录的工作流指纹不同则返回 ErrConflict。
	CreateIfAbsent(rec *RunRecord, fingerprint string) (existing *RunRecord, created bool, err error)
	Update(rec *RunRecord) error
	Get(id string) (*RunRecord, error)
	// ListIncomplete 返回所有非终态记录（用于重启恢复）。
	ListIncomplete() ([]*RunRecord, error)
}

type storeData struct {
	Runs         map[string]*RunRecord `json:"runs"`         // idempotency key -> record
	Fingerprints map[string]string     `json:"fingerprints"` // idempotency key -> workflow fingerprint
}

func newStoreData() *storeData {
	return &storeData{
		Runs:         make(map[string]*RunRecord),
		Fingerprints: make(map[string]string),
	}
}

// clone 通过 JSON 深拷贝记录，避免调用方与存储共享可变状态。
func clone(rec *RunRecord) (*RunRecord, error) {
	b, err := json.Marshal(rec)
	if err != nil {
		return nil, err
	}
	var out RunRecord
	if err := json.Unmarshal(b, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

func createIfAbsent(d *storeData, rec *RunRecord, fingerprint string) (*RunRecord, bool, error) {
	if existing, ok := d.Runs[rec.IdempotencyKey]; ok {
		if d.Fingerprints[rec.IdempotencyKey] != fingerprint {
			return nil, false, ErrConflict
		}
		cp, err := clone(existing)
		return cp, false, err
	}
	cp, err := clone(rec)
	if err != nil {
		return nil, false, err
	}
	d.Runs[rec.IdempotencyKey] = cp
	d.Fingerprints[rec.IdempotencyKey] = fingerprint
	out, err := clone(rec)
	return out, true, err
}

func update(d *storeData, rec *RunRecord) error {
	if _, ok := d.Runs[rec.IdempotencyKey]; !ok {
		return ErrNotFound
	}
	cp, err := clone(rec)
	if err != nil {
		return err
	}
	d.Runs[rec.IdempotencyKey] = cp
	return nil
}

func get(d *storeData, key string) (*RunRecord, error) {
	rec, ok := d.Runs[key]
	if !ok {
		return nil, ErrNotFound
	}
	return clone(rec)
}

func listIncomplete(d *storeData) ([]*RunRecord, error) {
	var out []*RunRecord
	for _, rec := range d.Runs {
		if rec.Status.Terminal() {
			continue
		}
		cp, err := clone(rec)
		if err != nil {
			return nil, err
		}
		out = append(out, cp)
	}
	return out, nil
}

// FileStore 是基于 JSON 文件的 Store 实现（单文件、进程内加锁、原子替换写入）。
type FileStore struct {
	mu   sync.Mutex
	path string
	data *storeData
}

// NewFileStore 打开（或创建）一个文件存储。
func NewFileStore(path string) (*FileStore, error) {
	s := &FileStore{path: path, data: newStoreData()}
	b, err := os.ReadFile(path)
	switch {
	case errors.Is(err, os.ErrNotExist):
		// 空存储
	case err != nil:
		return nil, err
	default:
		if len(b) > 0 {
			if err := json.Unmarshal(b, s.data); err != nil {
				return nil, fmt.Errorf("engine: corrupt store file %s: %w", path, err)
			}
		}
	}
	return s, nil
}

// flush 将数据原子写入磁盘（临时文件 + rename）。
func (s *FileStore) flushLocked() error {
	b, err := json.MarshalIndent(s.data, "", "  ")
	if err != nil {
		return err
	}
	tmp := s.path + ".tmp"
	if err := os.MkdirAll(filepath.Dir(s.path), 0o755); err != nil {
		return err
	}
	if err := os.WriteFile(tmp, b, 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, s.path)
}

func (s *FileStore) CreateIfAbsent(rec *RunRecord, fingerprint string) (*RunRecord, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	out, created, err := createIfAbsent(s.data, rec, fingerprint)
	if err != nil || !created {
		return out, created, err
	}
	if err := s.flushLocked(); err != nil {
		return nil, false, err
	}
	return out, true, nil
}

func (s *FileStore) Update(rec *RunRecord) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := update(s.data, rec); err != nil {
		return err
	}
	return s.flushLocked()
}

func (s *FileStore) Get(id string) (*RunRecord, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return get(s.data, id)
}

func (s *FileStore) ListIncomplete() ([]*RunRecord, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return listIncomplete(s.data)
}

// MemStore 是内存实现，供测试使用。
type MemStore struct {
	mu   sync.Mutex
	data *storeData
}

// NewMemStore 创建内存存储。
func NewMemStore() *MemStore {
	return &MemStore{data: newStoreData()}
}

func (s *MemStore) CreateIfAbsent(rec *RunRecord, fingerprint string) (*RunRecord, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return createIfAbsent(s.data, rec, fingerprint)
}

func (s *MemStore) Update(rec *RunRecord) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return update(s.data, rec)
}

func (s *MemStore) Get(id string) (*RunRecord, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return get(s.data, id)
}

func (s *MemStore) ListIncomplete() ([]*RunRecord, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return listIncomplete(s.data)
}
