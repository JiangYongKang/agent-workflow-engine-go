package workflow

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
)

// ErrVersionConflict 表示持久化时的乐观锁版本冲突。
var ErrVersionConflict = errors.New("execution version conflict")

// clone 深拷贝执行记录，避免存储与运行期共享可变状态。
func clone(e *Execution) *Execution {
	b, err := json.Marshal(e)
	if err != nil {
		panic(err)
	}
	var out Execution
	if err := json.Unmarshal(b, &out); err != nil {
		panic(err)
	}
	return &out
}

// Store 是执行状态的持久化接口。
type Store interface {
	// Save 持久化执行记录（按 ID 覆盖，需校验版本）。
	Save(exec *Execution) error
	// Load 按执行 ID 读取。
	Load(id string) (*Execution, error)
	// LoadByIdemKey 按幂等键读取，未找到返回 nil, nil。
	LoadByIdemKey(key string) (*Execution, error)
	// ListUnfinished 返回所有非终态执行，用于崩溃恢复。
	ListUnfinished() ([]*Execution, error)
	// ListResumes 返回同一续跑链（RootID 相同）的全部执行记录，
	// 按开始时间升序，便于审计追溯。
	ListResumes(rootID string) ([]*Execution, error)
}

// MemoryStore 是进程内 Store 实现。
type MemoryStore struct {
	mu     sync.Mutex
	byID   map[string]*Execution
	byIdem map[string]string
}

// NewMemoryStore 创建内存存储。
func NewMemoryStore() *MemoryStore {
	return &MemoryStore{byID: map[string]*Execution{}, byIdem: map[string]string{}}
}

func (m *MemoryStore) Save(exec *Execution) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if old, ok := m.byID[exec.ID]; ok && old.Version != exec.Version {
		return ErrVersionConflict
	}
	exec.Version++
	m.byID[exec.ID] = clone(exec)
	m.byIdem[exec.IdemKey] = exec.ID
	return nil
}

func (m *MemoryStore) Load(id string) (*Execution, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	e, ok := m.byID[id]
	if !ok {
		return nil, nil
	}
	return clone(e), nil
}

func (m *MemoryStore) LoadByIdemKey(key string) (*Execution, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	id, ok := m.byIdem[key]
	if !ok {
		return nil, nil
	}
	return clone(m.byID[id]), nil
}

func (m *MemoryStore) ListUnfinished() ([]*Execution, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var out []*Execution
	for _, e := range m.byID {
		if !e.Status.Terminal() {
			out = append(out, clone(e))
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].StartedAt.Before(out[j].StartedAt) })
	return out, nil
}

func (m *MemoryStore) ListResumes(rootID string) ([]*Execution, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var out []*Execution
	for _, e := range m.byID {
		if e.RootID == rootID {
			out = append(out, clone(e))
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].StartedAt.Before(out[j].StartedAt) })
	return out, nil
}

// FileStore 是基于 JSON 文件的 Store 实现，进程重启后可恢复。
type FileStore struct {
	dir string
	mu  sync.Mutex
}

// NewFileStore 创建文件存储。
func NewFileStore(dir string) (*FileStore, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	return &FileStore{dir: dir}, nil
}

func (f *FileStore) path(id string) string {
	return filepath.Join(f.dir, "exec-"+id+".json")
}

func (f *FileStore) Save(exec *Execution) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if old, err := f.loadLocked(exec.ID); err != nil {
		return err
	} else if old != nil && old.Version != exec.Version {
		return ErrVersionConflict
	}
	exec.Version++
	b, err := json.MarshalIndent(exec, "", "  ")
	if err != nil {
		return err
	}
	tmp := f.path(exec.ID) + ".tmp"
	if err := os.WriteFile(tmp, b, 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, f.path(exec.ID)) // 原子替换，避免半截文件
}

func (f *FileStore) loadLocked(id string) (*Execution, error) {
	b, err := os.ReadFile(f.path(id))
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var e Execution
	if err := json.Unmarshal(b, &e); err != nil {
		return nil, fmt.Errorf("corrupt execution file %s: %w", id, err)
	}
	return &e, nil
}

func (f *FileStore) Load(id string) (*Execution, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.loadLocked(id)
}

func (f *FileStore) LoadByIdemKey(key string) (*Execution, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	entries, err := os.ReadDir(f.dir)
	if err != nil {
		return nil, err
	}
	for _, en := range entries {
		if !strings.HasPrefix(en.Name(), "exec-") || !strings.HasSuffix(en.Name(), ".json") {
			continue
		}
		e, err := f.loadLocked(strings.TrimSuffix(strings.TrimPrefix(en.Name(), "exec-"), ".json"))
		if err != nil {
			return nil, err
		}
		if e != nil && e.IdemKey == key {
			return e, nil
		}
	}
	return nil, nil
}

func (f *FileStore) ListUnfinished() ([]*Execution, error) {
	return f.listMatching(func(e *Execution) bool { return !e.Status.Terminal() })
}

func (f *FileStore) ListResumes(rootID string) ([]*Execution, error) {
	return f.listMatching(func(e *Execution) bool { return e.RootID == rootID })
}

// listMatching 扫描目录下全部执行记录并按条件过滤，结果按开始时间升序。
func (f *FileStore) listMatching(match func(*Execution) bool) ([]*Execution, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	entries, err := os.ReadDir(f.dir)
	if err != nil {
		return nil, err
	}
	var out []*Execution
	for _, en := range entries {
		if !strings.HasPrefix(en.Name(), "exec-") || !strings.HasSuffix(en.Name(), ".json") {
			continue
		}
		e, err := f.loadLocked(strings.TrimSuffix(strings.TrimPrefix(en.Name(), "exec-"), ".json"))
		if err != nil {
			return nil, err
		}
		if e != nil && match(e) {
			out = append(out, e)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].StartedAt.Before(out[j].StartedAt) })
	return out, nil
}
