package quota

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
)

// Tx 是一次对额度数据的读写事务。所有 Put 都带版本条件：
// 期望版本与存储版本不一致时返回 ErrVersionConflict，从而避免丢失更新。
type Tx struct {
	state *fileState
}

func configKey(level Level, subjectID string) string {
	return string(level) + "/" + subjectID
}

func usageKey(level Level, subjectID string, windowStart int64) string {
	return fmt.Sprintf("%s/%s/%d", level, subjectID, windowStart)
}

// GetConfig 读取配额配置，不存在时 ok=false。
func (tx *Tx) GetConfig(level Level, subjectID string) (cfg QuotaConfig, ok bool) {
	cfg, ok = tx.state.Configs[configKey(level, subjectID)]
	return cfg, ok
}

// PutConfig 以 expectedVersion 为条件写入配置；新建时传 0。
func (tx *Tx) PutConfig(cfg QuotaConfig, expectedVersion int64) error {
	key := configKey(cfg.Level, cfg.SubjectID)
	cur, ok := tx.state.Configs[key]
	if ok && cur.Version != expectedVersion {
		return fmt.Errorf("%w: config %s stored=%d expected=%d",
			ErrVersionConflict, key, cur.Version, expectedVersion)
	}
	if !ok && expectedVersion != 0 {
		return fmt.Errorf("%w: config %s absent, expected version %d",
			ErrVersionConflict, key, expectedVersion)
	}
	cfg.Version = expectedVersion + 1
	tx.state.Configs[key] = cfg
	return nil
}

// GetUsage 读取某主体在指定窗口的用量，不存在时 ok=false。
func (tx *Tx) GetUsage(level Level, subjectID string, windowStart int64) (rec UsageRecord, ok bool) {
	rec, ok = tx.state.Usages[usageKey(level, subjectID, windowStart)]
	return rec, ok
}

// PutUsage 以 expectedVersion 为条件写入用量；新建时传 0。
func (tx *Tx) PutUsage(rec UsageRecord, expectedVersion int64) error {
	key := usageKey(rec.Level, rec.SubjectID, rec.WindowStart)
	cur, ok := tx.state.Usages[key]
	if ok && cur.Version != expectedVersion {
		return fmt.Errorf("%w: usage %s stored=%d expected=%d",
			ErrVersionConflict, key, cur.Version, expectedVersion)
	}
	if !ok && expectedVersion != 0 {
		return fmt.Errorf("%w: usage %s absent, expected version %d",
			ErrVersionConflict, key, expectedVersion)
	}
	rec.Version = expectedVersion + 1
	tx.state.Usages[key] = rec
	return nil
}

// GetConsume 按外部请求号读取消费记录。
func (tx *Tx) GetConsume(requestID string) (rec ConsumeRecord, ok bool) {
	rec, ok = tx.state.Consumes[requestID]
	return rec, ok
}

// PutConsume 以 expectedVersion 为条件写入消费记录；新建时传 0。
func (tx *Tx) PutConsume(rec ConsumeRecord, expectedVersion int64) error {
	cur, ok := tx.state.Consumes[rec.RequestID]
	if ok && cur.Version != expectedVersion {
		return fmt.Errorf("%w: consume %s stored=%d expected=%d",
			ErrVersionConflict, rec.RequestID, cur.Version, expectedVersion)
	}
	if !ok && expectedVersion != 0 {
		return fmt.Errorf("%w: consume %s absent, expected version %d",
			ErrVersionConflict, rec.RequestID, expectedVersion)
	}
	rec.Version = expectedVersion + 1
	tx.state.Consumes[rec.RequestID] = rec
	return nil
}

// GetRefund 按外部请求号读取退还记录。
func (tx *Tx) GetRefund(requestID string) (rec RefundRecord, ok bool) {
	rec, ok = tx.state.Refunds[requestID]
	return rec, ok
}

// PutRefund 写入退还记录（请求号即主键，不允许覆盖）。
func (tx *Tx) PutRefund(rec RefundRecord) error {
	if _, ok := tx.state.Refunds[rec.RequestID]; ok {
		return fmt.Errorf("%w: refund %s already exists", ErrVersionConflict, rec.RequestID)
	}
	tx.state.Refunds[rec.RequestID] = rec
	return nil
}

// GetReservation 按外部请求号读取预占记录。
func (tx *Tx) GetReservation(requestID string) (rec ReservationRecord, ok bool) {
	rec, ok = tx.state.Reservations[requestID]
	return rec, ok
}

// PutReservation 以 expectedVersion 为条件写入预占记录；新建时传 0。
func (tx *Tx) PutReservation(rec ReservationRecord, expectedVersion int64) error {
	cur, ok := tx.state.Reservations[rec.RequestID]
	if ok && cur.Version != expectedVersion {
		return fmt.Errorf("%w: reservation %s stored=%d expected=%d",
			ErrVersionConflict, rec.RequestID, cur.Version, expectedVersion)
	}
	if !ok && expectedVersion != 0 {
		return fmt.Errorf("%w: reservation %s absent, expected version %d",
			ErrVersionConflict, rec.RequestID, expectedVersion)
	}
	rec.Version = expectedVersion + 1
	tx.state.Reservations[rec.RequestID] = rec
	return nil
}

// GetRebalance 按外部请求号读取重平衡单。
func (tx *Tx) GetRebalance(requestID string) (rec RebalanceRecord, ok bool) {
	rec, ok = tx.state.Rebalances[requestID]
	return rec, ok
}

// PutRebalance 以 expectedVersion 为条件写入重平衡单；新建时传 0。
func (tx *Tx) PutRebalance(rec RebalanceRecord, expectedVersion int64) error {
	cur, ok := tx.state.Rebalances[rec.RequestID]
	if ok && cur.Version != expectedVersion {
		return fmt.Errorf("%w: rebalance %s stored=%d expected=%d",
			ErrVersionConflict, rec.RequestID, cur.Version, expectedVersion)
	}
	if !ok && expectedVersion != 0 {
		return fmt.Errorf("%w: rebalance %s absent, expected version %d",
			ErrVersionConflict, rec.RequestID, expectedVersion)
	}
	rec.Version = expectedVersion + 1
	tx.state.Rebalances[rec.RequestID] = rec
	return nil
}

// ListRebalances 列出全部重平衡单，供历史查询使用。
// 返回的记录顺序不做保证，调用方需自行排序。
func (tx *Tx) ListRebalances() []RebalanceRecord {
	out := make([]RebalanceRecord, 0, len(tx.state.Rebalances))
	for _, rec := range tx.state.Rebalances {
		out = append(out, rec)
	}
	return out
}

// ListConsumes 列出全部消费记录（含预占确认生成的消费），供消费历史查询使用。
// 返回的记录顺序不做保证，调用方需自行排序。
func (tx *Tx) ListConsumes() []ConsumeRecord {
	out := make([]ConsumeRecord, 0, len(tx.state.Consumes))
	for _, rec := range tx.state.Consumes {
		out = append(out, rec)
	}
	return out
}

// ListReservations 列出全部预占记录，供到期回收扫描使用。
// 返回的记录顺序不做保证，调用方不得依赖。
func (tx *Tx) ListReservations() []ReservationRecord {
	out := make([]ReservationRecord, 0, len(tx.state.Reservations))
	for _, rec := range tx.state.Reservations {
		out = append(out, rec)
	}
	return out
}

// Store 提供事务化的额度数据访问。
type Store interface {
	// Update 在写事务中执行 fn；fn 返回错误则整体回滚，
	// 成功则把全部变更作为一个整体持久化。
	Update(ctx context.Context, fn func(tx *Tx) error) error
	// View 在读事务中执行 fn。
	View(ctx context.Context, fn func(tx *Tx) error) error
}

// fileState 是持久化到磁盘的全部业务数据。
type fileState struct {
	Configs      map[string]QuotaConfig       `json:"configs"`
	Usages       map[string]UsageRecord       `json:"usages"`
	Consumes     map[string]ConsumeRecord     `json:"consumes"`
	Refunds      map[string]RefundRecord      `json:"refunds"`
	Reservations map[string]ReservationRecord `json:"reservations"`
	Rebalances   map[string]RebalanceRecord   `json:"rebalances"`
}

func newFileState() *fileState {
	return &fileState{
		Configs:      map[string]QuotaConfig{},
		Usages:       map[string]UsageRecord{},
		Consumes:     map[string]ConsumeRecord{},
		Refunds:      map[string]RefundRecord{},
		Reservations: map[string]ReservationRecord{},
		Rebalances:   map[string]RebalanceRecord{},
	}
}

// FileStore 是基于 JSON 文件的 Store 实现：进程内用互斥锁串行化事务，
// 每次写事务成功后通过临时文件 + rename 原子落盘。
type FileStore struct {
	mu    sync.Mutex
	path  string
	state *fileState
}

// OpenFileStore 打开（必要时创建）位于 path 的存储。
func OpenFileStore(path string) (*FileStore, error) {
	s := &FileStore{path: path, state: newFileState()}
	data, err := os.ReadFile(path)
	switch {
	case err == nil:
		if err := json.Unmarshal(data, s.state); err != nil {
			return nil, fmt.Errorf("decode store %s: %w", path, err)
		}
		// 兼容旧版本数据文件：补齐后加的字段，避免后续写入空 map。
		if s.state.Rebalances == nil {
			s.state.Rebalances = map[string]RebalanceRecord{}
		}
	case os.IsNotExist(err):
		// 首次运行，使用空状态。
	default:
		return nil, fmt.Errorf("read store %s: %w", path, err)
	}
	return s, nil
}

// NewMemoryStore 返回不落盘的 Store，便于测试。
func NewMemoryStore() *FileStore {
	return &FileStore{state: newFileState()}
}

func (s *fileState) clone() *fileState {
	c := newFileState()
	for k, v := range s.Configs {
		c.Configs[k] = v
	}
	for k, v := range s.Usages {
		c.Usages[k] = v
	}
	for k, v := range s.Consumes {
		c.Consumes[k] = v
	}
	for k, v := range s.Refunds {
		c.Refunds[k] = v
	}
	for k, v := range s.Reservations {
		c.Reservations[k] = v
	}
	for k, v := range s.Rebalances {
		c.Rebalances[k] = v
	}
	return c
}

func (s *FileStore) Update(ctx context.Context, fn func(tx *Tx) error) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	// 在状态副本上执行，fn 失败时直接丢弃副本，天然回滚。
	work := s.state.clone()
	if err := fn(&Tx{state: work}); err != nil {
		return err
	}
	s.state = work
	if s.path == "" {
		return nil
	}
	return s.persistLocked()
}

func (s *FileStore) View(ctx context.Context, fn func(tx *Tx) error) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return fn(&Tx{state: s.state})
}

// persistLocked 先写临时文件再 rename，保证崩溃时不会留下半个文件。
func (s *FileStore) persistLocked() error {
	data, err := json.MarshalIndent(s.state, "", "  ")
	if err != nil {
		return err
	}
	tmp := s.path + ".tmp"
	if err := os.WriteFile(tmp, data, 0o600); err != nil {
		return err
	}
	if err := os.Rename(tmp, s.path); err != nil {
		return err
	}
	if dir, err := os.Open(filepath.Dir(s.path)); err == nil {
		_ = dir.Sync()
		_ = dir.Close()
	}
	return nil
}
