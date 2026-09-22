package dpengine

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
)

// CurrentFormatVersion 是当前持久化格式版本。
//
// 兼容范围与策略：
//   - 缺失 version 字段或 version==1 的快照：按版本 1 语义加载，
//     缺失的新字段使用确定的零值默认（见 normalizeLoadedState）；
//   - version > CurrentFormatVersion：显式拒绝（ErrIncompatibleVersion），
//     不做静默降级；
//   - version 非法（<=0）：按损坏数据拒绝（ErrCorruptState）。
const CurrentFormatVersion = 1

// minSupportedFormatVersion 是可加载的最低版本。
const minSupportedFormatVersion = 1

// storedState 是持久化快照的磁盘格式。
type storedState struct {
	FormatVersion int                     `json:"format_version"`
	Ledger        *Ledger                 `json:"ledger"`
	Results       map[string]*CountResult `json:"results,omitempty"`
}

// saveState 将内存状态原子写入 path：先写临时文件并 fsync，再 rename 覆盖，
// 保证崩溃时快照要么是旧版本要么是新版本，不会出现半写文件。
func saveState(path string, ledger *Ledger, results map[string]*CountResult) error {
	if path == "" {
		return nil // 未配置持久化：纯内存模式
	}
	st := storedState{
		FormatVersion: CurrentFormatVersion,
		Ledger:        ledger,
		Results:       results,
	}
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetIndent("", "  ")
	if err := enc.Encode(&st); err != nil {
		return wrapError(ErrCorruptState, err, "encode ledger snapshot")
	}

	dir := filepath.Dir(path)
	tmp, err := os.CreateTemp(dir, ".dpengine-*.tmp")
	if err != nil {
		return wrapError(ErrCorruptState, err, "create temp snapshot in %q", dir)
	}
	tmpName := tmp.Name()
	cleanup := func() { _ = os.Remove(tmpName) }

	if _, err := tmp.Write(buf.Bytes()); err != nil {
		_ = tmp.Close()
		cleanup()
		return wrapError(ErrCorruptState, err, "write temp snapshot %q", tmpName)
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		cleanup()
		return wrapError(ErrCorruptState, err, "fsync temp snapshot %q", tmpName)
	}
	if err := tmp.Close(); err != nil {
		cleanup()
		return wrapError(ErrCorruptState, err, "close temp snapshot %q", tmpName)
	}
	if err := os.Rename(tmpName, path); err != nil {
		cleanup()
		return wrapError(ErrCorruptState, err, "rename snapshot to %q", path)
	}
	// 尽力 fsync 目录，保证 rename 在掉电后持久化。
	if d, err := os.Open(dir); err == nil {
		_ = d.Sync()
		_ = d.Close()
	}
	return nil
}

// loadState 从 path 读取快照。文件不存在视为全新空账本（非错误）。
func loadState(path string) (*Ledger, map[string]*CountResult, bool, error) {
	if path == "" {
		return NewLedger(), map[string]*CountResult{}, false, nil
	}
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return NewLedger(), map[string]*CountResult{}, false, nil
		}
		return nil, nil, false, wrapError(ErrCorruptState, err, "read snapshot %q", path)
	}

	var st storedState
	dec := json.NewDecoder(bytes.NewReader(data))
	if err := dec.Decode(&st); err != nil {
		return nil, nil, false, wrapError(ErrCorruptState, err, "decode snapshot %q", path)
	}

	// 旧数据缺失 format_version：JSON 零值为 0，按最稳定的历史默认
	// （版本 1）加载。
	loadedVersion := st.FormatVersion
	if loadedVersion == 0 {
		loadedVersion = 1
	}
	if loadedVersion < minSupportedFormatVersion {
		return nil, nil, false, errorf(ErrCorruptState,
			"snapshot format version %d is invalid (minimum supported: %d)",
			loadedVersion, minSupportedFormatVersion)
	}
	if loadedVersion > CurrentFormatVersion {
		return nil, nil, false, errorf(ErrIncompatibleVersion,
			"snapshot format version %d is newer than engine supported version %d; refusing to load",
			loadedVersion, CurrentFormatVersion)
	}

	if err := normalizeLoadedState(&st); err != nil {
		return nil, nil, false, err
	}
	return st.Ledger, st.Results, true, nil
}

// normalizeLoadedState 修复旧快照中缺失新字段时的确定默认语义，并对
// 恢复出的状态做一致性校验；任何语义不合法都显式拒绝。
func normalizeLoadedState(st *storedState) error {
	if st.Ledger == nil {
		st.Ledger = NewLedger()
	}
	if st.Ledger.Datasets == nil {
		st.Ledger.Datasets = map[string]*DatasetBudget{}
	}
	if st.Ledger.Requests == nil {
		st.Ledger.Requests = map[string]string{}
	}
	if st.Results == nil {
		st.Results = map[string]*CountResult{}
	}

	for name, db := range st.Ledger.Datasets {
		if db == nil {
			return errorf(ErrCorruptState, "dataset %q snapshot is null", name)
		}
		if db.Name == "" {
			db.Name = name // 旧格式缺失内联名称时按键名稳定恢复
		}
		if db.Events == nil {
			db.Events = []Event{}
		}
		// 无论旧快照是否包含 NextSeq，都从事件重放重建，保证序列号
		// 与事件列表严格一致。
		var maxSeq int64
		var epsUsed, deltaUsed float64
		for i, ev := range db.Events {
			if ev.Seq <= 0 {
				return errorf(ErrCorruptState,
					"dataset %q event[%d] has non-positive seq %d", name, i, ev.Seq)
			}
			if ev.Seq > maxSeq {
				maxSeq = ev.Seq
			}
			if !isFinite(ev.Epsilon) || !isFinite(ev.Delta) ||
				ev.Epsilon < 0 || ev.Delta < 0 {
				return errorf(ErrCorruptState,
					"dataset %q event[%d] has invalid charge eps=%v delta=%v",
					name, i, ev.Epsilon, ev.Delta)
			}
			epsUsed += ev.Epsilon
			deltaUsed += ev.Delta
			if ev.RequestID == "" {
				return errorf(ErrCorruptState,
					"dataset %q event[%d] missing request_id", name, i)
			}
		}
		db.NextSeq = maxSeq
		// 已用预算以事件重放为准；旧快照若字段缺失或与事件不一致，
		// 以事件累加值恢复，使崩溃后预算状态确定唯一。
		db.EpsilonUsed = epsUsed
		db.DeltaUsed = deltaUsed
		if !isFinite(db.EpsilonBudget) || db.EpsilonBudget <= 0 {
			return errorf(ErrCorruptState,
				"dataset %q has invalid epsilon budget %v", name, db.EpsilonBudget)
		}
		if epsUsed > db.EpsilonBudget+budgetTolerance ||
			deltaUsed > db.DeltaBudget+budgetTolerance {
			return errorf(ErrCorruptState,
				"dataset %q snapshot over budget: used eps=%v/%v delta=%v/%v",
				name, epsUsed, db.EpsilonBudget, deltaUsed, db.DeltaBudget)
		}
	}

	// 重放去重索引：以事件为准重建，保证重启后重复请求仍被识别。
	replayed := map[string]string{}
	for name, db := range st.Ledger.Datasets {
		for _, ev := range db.Events {
			if owner, dup := replayed[ev.RequestID]; dup {
				return errorf(ErrCorruptState,
					"request_id %q appears in both %q and %q", ev.RequestID, owner, name)
			}
			replayed[ev.RequestID] = name
		}
	}
	st.Ledger.Requests = replayed

	// 结果缓存必须与账本事件一一对应；孤儿结果或字段非法直接拒绝。
	for rid, res := range st.Results {
		if res == nil {
			return errorf(ErrCorruptState, "cached result for %q is null", rid)
		}
		if _, ok := replayed[rid]; !ok {
			return errorf(ErrCorruptState,
				"cached result for %q has no matching ledger event", rid)
		}
		if !isFinite(res.NoisyValue) || !isFinite(res.NoiseScale) || res.NoiseScale <= 0 {
			return errorf(ErrCorruptState,
				"cached result for %q has invalid noise values", rid)
		}
	}
	return nil
}
