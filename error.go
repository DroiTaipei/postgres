package postgres

import (
	"github.com/DroiTaipei/droipkg"
	"github.com/devopstaku/gorm"
	"github.com/lib/pq"
)

type RdbApiError struct {
	Code int
	Err  error
}

func (ae *RdbApiError) ErrorCode() int {
	return ae.Code
}
func (ae *RdbApiError) SetErrorCode(code int) {
	ae.Code = code
}
func (ae *RdbApiError) Error() string {
	return ae.Err.Error()
}
func (ae *RdbApiError) Wrap(msg string) {
	ae.Err = droipkg.Wrap(ae.Err, msg)
}

const (
	UnknowErrorCode               = 1020001
	ResouceNotFoundCode           = 1020002
	ParameterValidationFailedCode = 1020003
	JsonValidationFailedCode      = 1020004
	PermissionDeniedCode          = 1020005
	DatabaseUnavailableCode       = 1020006
	DatabaseErrorCode             = 1020007
	DataProcessFailedCode         = 1020008
	PrimaryKeyDuplicatedCode      = 1020009
	DataNotFoundCode              = 1020010
	CacheKeyLimitCode             = 1020011
	CacheValueLimitCode           = 1020012
	CacheNotFoundCode             = 1020013
	CacheUpdateFailedCode         = 1020014
	CacheDeleteFailedCode         = 1020015
	OutCacheRecordLimitCode       = 1020016 //(Upper bound is 100)
	DuplicatedTableCode           = 1020017
)

var (
	InitializeFailed     droipkg.DroiError
	InvalidParameter     droipkg.DroiError
	ResourceNotFound     droipkg.DroiError
	DatabaseUnavailable  droipkg.DroiError
	DatabaseErr          droipkg.DroiError
	DataNotFound         droipkg.DroiError
	ProcessFailed        droipkg.DroiError
	DuplicatedTable      droipkg.DroiError
	PrimaryKeyDuplicated droipkg.DroiError
	ServerErr            droipkg.DroiError
	errorCodeMap         map[pq.ErrorCode]droipkg.DroiError
)

func init() {
	InitializeFailed = &RdbApiError{
		Code: UnknowErrorCode,
		Err:  droipkg.NewError("Initialize Failed"),
	}
	InvalidParameter = &RdbApiError{
		Code: ParameterValidationFailedCode,
		Err:  droipkg.NewError("Invalid Parameter"),
	}

	ResourceNotFound = &RdbApiError{
		Code: ResouceNotFoundCode,
		Err:  droipkg.NewError("Resource Not Found"),
	}

	DatabaseUnavailable = &RdbApiError{
		Code: DatabaseUnavailableCode,
		Err:  droipkg.NewError("Database Unavailable"),
	}

	ServerErr = &RdbApiError{
		Code: DatabaseErrorCode,
		Err:  droipkg.NewError("Server Execution Error"),
	}

	DataNotFound = &RdbApiError{
		Code: DataNotFoundCode,
		Err:  droipkg.NewError("Data Not Found"),
	}
	ProcessFailed = &RdbApiError{
		Code: DataProcessFailedCode,
		Err:  droipkg.NewError("Processing Failed"),
	}

	DuplicatedTable = &RdbApiError{
		Code: PrimaryKeyDuplicatedCode,
		Err:  droipkg.NewError("Duplicated Table"),
	}

	PrimaryKeyDuplicated = &RdbApiError{
		Code: PrimaryKeyDuplicatedCode,
		Err:  droipkg.NewError("Primary Key Duplicated"),
	}

	errorCodeMap = map[pq.ErrorCode]droipkg.DroiError{
		"42P01": ResourceNotFound,
		"42P07": DuplicatedTable,
		"23505": PrimaryKeyDuplicated,
	}
}

func checkDatabaseError(err error, errPtr *droipkg.DroiError) {
	// TODO: More detail error handling
	if err != nil {

		switch err {
		case gorm.ErrRecordNotFound:
			*errPtr = DataNotFound
		case gorm.ErrInvalidSQL:
			*errPtr = ProcessFailed
		default:
			*errPtr = DatabaseErr

			if pe, ok := err.(*pq.Error); ok {
				aErr, handled := errorCodeMap[pe.Code]
				if handled {
					*errPtr = aErr
				} else {
					debug("Unhandle:", pe.Code)
				}
			}
		}
		droipkg.Wrap(*errPtr, err.Error())
	}
}
