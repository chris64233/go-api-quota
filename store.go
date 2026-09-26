package apiquota

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
)

// Store 持久化全部业务数据。数据以 JSON 文件形式原子落盘
// （临时文件 + rename），path 为空时退化为纯内存存储，便于测试。
type Store struct {
	mu   sync.Mutex
	path string
	data dataFile
}

type dataFile struct {
	Quotas       map[string]QuotaConfig       `json:"quotas"`
	Usages       map[string]UsageRecord       `json:"usages"`
	Consumptions map[string]Consumption       `json:"consumptions"`
	Idempotency  map[string]IdempotencyRecord `json:"idempotency"`
}

func newDataFile() dataFile {
	return dataFile{
		Quotas:       make(map[string]QuotaConfig),
		Usages:       make(map[string]UsageRecord),
		Consumptions: make(map[string]Consumption),
		Idempotency:  make(map[string]IdempotencyRecord),
	}
}

// OpenStore 打开（或创建）位于 path 的持久化存储。
func OpenStore(path string) (*Store, error) {
	s := &Store{path: path, data: newDataFile()}
	b, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return s, nil
		}
		return nil, fmt.Errorf("read store: %w", err)
	}
	if len(b) > 0 {
		if err := json.Unmarshal(b, &s.data); err != nil {
			return nil, fmt.Errorf("decode store: %w", err)
		}
	}
	return s, nil
}

// NewMemoryStore 返回不落盘的内存存储。
func NewMemoryStore() *Store { return &Store{data: newDataFile()} }

// Transact 在互斥保护下执行 fn，fn 成功后将数据原子落盘。
// fn 返回错误时不会产生任何持久化副作用。
func (s *Store) Transact(fn func(*Tx) error) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	tx := &Tx{s: s}
	if err := fn(tx); err != nil {
		return err
	}
	return s.saveLocked()
}

func (s *Store) saveLocked() error {
	if s.path == "" {
		return nil
	}
	b, err := json.MarshalIndent(s.data, "", "  ")
	if err != nil {
		return fmt.Errorf("encode store: %w", err)
	}
	tmp := filepath.Join(filepath.Dir(s.path), ".tmp-"+filepath.Base(s.path))
	if err := os.WriteFile(tmp, b, 0o644); err != nil {
		return fmt.Errorf("write store: %w", err)
	}
	if err := os.Rename(tmp, s.path); err != nil {
		return fmt.Errorf("commit store: %w", err)
	}
	return nil
}

// Tx 是一次事务内对存储的视图，所有写入均带版本条件。
type Tx struct{ s *Store }

func quotaKey(level Level, entityID string) string { return string(level) + "|" + entityID }

func usageKey(level Level, entityID string, wsUnix int64) string {
	return fmt.Sprintf("%s|%s|%d", level, entityID, wsUnix)
}

// GetQuota 读取配额配置。
func (tx *Tx) GetQuota(level Level, entityID string) (QuotaConfig, bool) {
	c, ok := tx.s.data.Quotas[quotaKey(level, entityID)]
	return c, ok
}

// PutQuota 以 expectedVersion 为条件写入配置；记录不存在时 expectedVersion 必须为 0。
func (tx *Tx) PutQuota(cfg QuotaConfig, expectedVersion int64) error {
	k := quotaKey(cfg.Level, cfg.EntityID)
	existing, ok := tx.s.data.Quotas[k]
	if ok && existing.Version != expectedVersion {
		return ErrVersionConflict
	}
	if !ok && expectedVersion != 0 {
		return ErrVersionConflict
	}
	cfg.Version = expectedVersion + 1
	tx.s.data.Quotas[k] = cfg
	return nil
}

// GetUsage 读取某实体在某窗口的用量记录。
func (tx *Tx) GetUsage(level Level, entityID string, wsUnix int64) (UsageRecord, bool) {
	u, ok := tx.s.data.Usages[usageKey(level, entityID, wsUnix)]
	return u, ok
}

// PutUsage 以 expectedVersion 为条件写入用量记录，防止并发下的丢失更新。
func (tx *Tx) PutUsage(u UsageRecord, expectedVersion int64) error {
	k := usageKey(u.Level, u.EntityID, u.WindowStart.Unix())
	existing, ok := tx.s.data.Usages[k]
	if ok && existing.Version != expectedVersion {
		return ErrVersionConflict
	}
	if !ok && expectedVersion != 0 {
		return ErrVersionConflict
	}
	u.Version = expectedVersion + 1
	tx.s.data.Usages[k] = u
	return nil
}

// GetConsumption 读取消费记录。
func (tx *Tx) GetConsumption(id string) (Consumption, bool) {
	c, ok := tx.s.data.Consumptions[id]
	return c, ok
}

// PutConsumption 以 expectedVersion 为条件写入消费记录。
func (tx *Tx) PutConsumption(c Consumption, expectedVersion int64) error {
	existing, ok := tx.s.data.Consumptions[c.ID]
	if ok && existing.Version != expectedVersion {
		return ErrVersionConflict
	}
	if !ok && expectedVersion != 0 {
		return ErrVersionConflict
	}
	c.Version = expectedVersion + 1
	tx.s.data.Consumptions[c.ID] = c
	return nil
}

// GetIdempotency 读取幂等记录。
func (tx *Tx) GetIdempotency(requestID string) (IdempotencyRecord, bool) {
	r, ok := tx.s.data.Idempotency[requestID]
	return r, ok
}

// PutIdempotency 写入幂等记录。请求号一旦写入不可覆盖。
func (tx *Tx) PutIdempotency(r IdempotencyRecord) error {
	if _, ok := tx.s.data.Idempotency[r.RequestID]; ok {
		return ErrVersionConflict
	}
	tx.s.data.Idempotency[r.RequestID] = r
	return nil
}
