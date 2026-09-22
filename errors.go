package dpengine

import "fmt"

// ErrorCode 是可机读、可区分的错误原因分类。
// 调用方应使用 HasCode 判定原因，而不是依赖错误文本。
type ErrorCode string

const (
	// ErrInvalidParams：隐私参数、数据集参数或查询参数非法（<=0、NaN、Inf、
	// 贡献上界缺失等）。
	ErrInvalidParams ErrorCode = "invalid_params"
	// ErrDatasetNotFound：查询引用了未注册的数据集。
	ErrDatasetNotFound ErrorCode = "dataset_not_found"
	// ErrDatasetExists：重复注册同名数据集。
	ErrDatasetExists ErrorCode = "dataset_exists"
	// ErrBudgetExceeded：本次消耗会使累计预算超出数据集预算上限。
	ErrBudgetExceeded ErrorCode = "budget_exceeded"
	// ErrUnboundedInput：查询缺少贡献上界，存在无界输入风险。
	ErrUnboundedInput ErrorCode = "unbounded_input"
	// ErrContributionRejected：策略为 Reject，且存在超出上界的用户贡献。
	ErrContributionRejected ErrorCode = "contribution_rejected"
	// ErrIncompatibleVersion：持久化数据版本高于本引擎支持范围，拒绝加载。
	ErrIncompatibleVersion ErrorCode = "incompatible_version"
	// ErrCorruptState：持久化数据损坏或语义不合法，拒绝加载。
	ErrCorruptState ErrorCode = "corrupt_state"
	// ErrInternal：不应当发生的内部状态错误。
	ErrInternal ErrorCode = "internal_error"
)

// EngineError 携带稳定错误码与人类可读信息；Cause 保留底层错误以便包装。
type EngineError struct {
	Code    ErrorCode
	Message string
	Cause   error
}

func (e *EngineError) Error() string {
	if e.Cause != nil {
		return fmt.Sprintf("dpengine[%s]: %s: %v", e.Code, e.Message, e.Cause)
	}
	return fmt.Sprintf("dpengine[%s]: %s", e.Code, e.Message)
}

func (e *EngineError) Unwrap() error { return e.Cause }

func errorf(code ErrorCode, format string, args ...any) *EngineError {
	return &EngineError{Code: code, Message: fmt.Sprintf(format, args...)}
}

func wrapError(code ErrorCode, cause error, format string, args ...any) *EngineError {
	return &EngineError{Code: code, Message: fmt.Sprintf(format, args...), Cause: cause}
}

// HasCode 报告 err（或其包装链中的 *EngineError）是否携带指定错误码。
func HasCode(err error, code ErrorCode) bool {
	for err != nil {
		if ee, ok := err.(*EngineError); ok {
			return ee.Code == code
		}
		type wrapper interface{ Unwrap() error }
		w, ok := err.(wrapper)
		if !ok {
			return false
		}
		err = w.Unwrap()
	}
	return false
}
