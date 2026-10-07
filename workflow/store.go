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

// cloneResume 深拷贝续跑记录。
func cloneResume(r *ResumeRecord) *ResumeRecord {
	b, err := json.Marshal(r)
	if err != nil {
		panic(err)
	}
	var out ResumeRecord
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

	// SaveResume 持久化续跑记录（按 ID 覆盖，需校验版本）。
	SaveResume(rec *ResumeRecord) error
	// LoadResume 按续跑记录 ID 读取，未找到返回 nil, nil。
	LoadResume(id string) (*ResumeRecord, error)
	// LoadResumeChain 返回某最初执行的全部续跑记录，按序号升序。
	LoadResumeChain(rootExecutionID string) ([]*ResumeRecord, error)
	// SaveResumeHead 以乐观锁原子更新 rootExecutionID 对应的链头续跑 ID；
	// 期望链头不存在时传 wantHead=""。冲突返回 ErrVersionConflict。
	SaveResumeHead(rootExecutionID, wantHead, newHead string) error
	// LoadResumeHead 读取链头续跑 ID；不存在返回 "", nil。
	LoadResumeHead(rootExecutionID string) (string, error)
	// ListUnfinishedResumes 返回所有非终态续跑记录，用于崩溃恢复。
	ListUnfinishedResumes() ([]*ResumeRecord, error)
}

// MemoryStore 是进程内 Store 实现。
type MemoryStore struct {
	mu       sync.Mutex
	byID     map[string]*Execution
	byIdem   map[string]string
	resumes  map[string]*ResumeRecord
	resHeads map[string]string // rootExecutionID -> 链头续跑 ID
	resVer   map[string]int64  // rootExecutionID -> 链头版本号（乐观锁）
}

// NewMemoryStore 创建内存存储。
func NewMemoryStore() *MemoryStore {
	return &MemoryStore{
		byID:     map[string]*Execution{},
		byIdem:   map[string]string{},
		resumes:  map[string]*ResumeRecord{},
		resHeads: map[string]string{},
		resVer:   map[string]int64{},
	}
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

func (m *MemoryStore) SaveResume(rec *ResumeRecord) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if old, ok := m.resumes[rec.ID]; ok && old.Version != rec.Version {
		return ErrVersionConflict
	}
	rec.Version++
	m.resumes[rec.ID] = cloneResume(rec)
	return nil
}

func (m *MemoryStore) LoadResume(id string) (*ResumeRecord, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	r, ok := m.resumes[id]
	if !ok {
		return nil, nil
	}
	return cloneResume(r), nil
}

func (m *MemoryStore) LoadResumeChain(rootExecutionID string) ([]*ResumeRecord, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return resumeChainLocked(m.resumes, m.resHeads[rootExecutionID], rootExecutionID), nil
}

// resumeChainLocked 从链头沿 ParentID 回溯，只返回真正在链上的续跑记录（按序号升序），
// CAS 落败产生的孤儿记录被排除。
func resumeChainLocked(all map[string]*ResumeRecord, headID, rootID string) []*ResumeRecord {
	id := headID
	var chain []*ResumeRecord
	for id != "" && id != rootID {
		r, ok := all[id]
		if !ok {
			break
		}
		chain = append(chain, r)
		id = r.ParentID
	}
	return chainFromHead(chain)
}

// chainFromHead 将“从链头回溯得到的新→旧记录”翻转为序号升序并深拷贝。
func chainFromHead(headToRoot []*ResumeRecord) []*ResumeRecord {
	out := make([]*ResumeRecord, len(headToRoot))
	for i, r := range headToRoot {
		out[len(headToRoot)-1-i] = cloneResume(r)
	}
	return out
}

func (m *MemoryStore) SaveResumeHead(rootExecutionID, wantHead, newHead string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if got := m.resHeads[rootExecutionID]; got != wantHead {
		return ErrVersionConflict
	}
	m.resHeads[rootExecutionID] = newHead
	m.resVer[rootExecutionID]++
	return nil
}

func (m *MemoryStore) LoadResumeHead(rootExecutionID string) (string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.resHeads[rootExecutionID], nil
}

func (m *MemoryStore) ListUnfinishedResumes() ([]*ResumeRecord, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var out []*ResumeRecord
	for _, r := range m.resumes {
		if !r.Status.Terminal() {
			out = append(out, cloneResume(r))
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
		if e != nil && !e.Status.Terminal() {
			out = append(out, e)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].StartedAt.Before(out[j].StartedAt) })
	return out, nil
}

func (f *FileStore) resumePath(id string) string {
	return filepath.Join(f.dir, "resume-"+id+".json")
}

func (f *FileStore) headPath(rootExecutionID string) string {
	return filepath.Join(f.dir, "resumehead-"+rootExecutionID+".json")
}

func (f *FileStore) loadResumeLocked(id string) (*ResumeRecord, error) {
	b, err := os.ReadFile(f.resumePath(id))
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var r ResumeRecord
	if err := json.Unmarshal(b, &r); err != nil {
		return nil, fmt.Errorf("corrupt resume file %s: %w", id, err)
	}
	return &r, nil
}

func (f *FileStore) SaveResume(rec *ResumeRecord) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if old, err := f.loadResumeLocked(rec.ID); err != nil {
		return err
	} else if old != nil && old.Version != rec.Version {
		return ErrVersionConflict
	}
	rec.Version++
	b, err := json.MarshalIndent(rec, "", "  ")
	if err != nil {
		return err
	}
	tmp := f.resumePath(rec.ID) + ".tmp"
	if err := os.WriteFile(tmp, b, 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, f.resumePath(rec.ID))
}

func (f *FileStore) LoadResume(id string) (*ResumeRecord, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.loadResumeLocked(id)
}

func (f *FileStore) LoadResumeChain(rootExecutionID string) ([]*ResumeRecord, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	b, err := os.ReadFile(f.headPath(rootExecutionID))
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var cur resumeHeadFile
	if err := json.Unmarshal(b, &cur); err != nil {
		return nil, fmt.Errorf("corrupt resume head for %s: %w", rootExecutionID, err)
	}
	var chain []*ResumeRecord
	id := cur.Head
	for id != "" && id != rootExecutionID {
		r, err := f.loadResumeLocked(id)
		if err != nil {
			return nil, err
		}
		if r == nil {
			break
		}
		chain = append(chain, r)
		id = r.ParentID
	}
	return chainFromHead(chain), nil
}

// resumeHeadFile 是链头指针的持久化载体，version 用作乐观锁。
type resumeHeadFile struct {
	Root    string `json:"root"`
	Head    string `json:"head"`
	Version int64  `json:"version"`
}

func (f *FileStore) SaveResumeHead(rootExecutionID, wantHead, newHead string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	var cur resumeHeadFile
	if b, err := os.ReadFile(f.headPath(rootExecutionID)); err == nil {
		if err := json.Unmarshal(b, &cur); err != nil {
			return fmt.Errorf("corrupt resume head for %s: %w", rootExecutionID, err)
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	if cur.Head != wantHead {
		return ErrVersionConflict
	}
	cur.Root = rootExecutionID
	cur.Head = newHead
	cur.Version++
	b, err := json.MarshalIndent(cur, "", "  ")
	if err != nil {
		return err
	}
	tmp := f.headPath(rootExecutionID) + ".tmp"
	if err := os.WriteFile(tmp, b, 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, f.headPath(rootExecutionID))
}

func (f *FileStore) LoadResumeHead(rootExecutionID string) (string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	b, err := os.ReadFile(f.headPath(rootExecutionID))
	if errors.Is(err, os.ErrNotExist) {
		return "", nil
	}
	if err != nil {
		return "", err
	}
	var cur resumeHeadFile
	if err := json.Unmarshal(b, &cur); err != nil {
		return "", fmt.Errorf("corrupt resume head for %s: %w", rootExecutionID, err)
	}
	return cur.Head, nil
}

func (f *FileStore) ListUnfinishedResumes() ([]*ResumeRecord, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	entries, err := os.ReadDir(f.dir)
	if err != nil {
		return nil, err
	}
	var out []*ResumeRecord
	for _, en := range entries {
		if !strings.HasPrefix(en.Name(), "resume-") || !strings.HasSuffix(en.Name(), ".json") {
			continue
		}
		r, err := f.loadResumeLocked(strings.TrimSuffix(strings.TrimPrefix(en.Name(), "resume-"), ".json"))
		if err != nil {
			return nil, err
		}
		if r != nil && !r.Status.Terminal() {
			out = append(out, r)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].StartedAt.Before(out[j].StartedAt) })
	return out, nil
}
