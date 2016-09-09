package postgres

import (
	"github.com/DroiTaipei/droipkg"
	"github.com/devopstaku/gorm"
	"github.com/lib/pq"
)

var (
	InitializeFailed     error
	InvalidParameter     error
	ResourceNotFound     error
	DatabaseUnavailable  error
	DatabaseErr          error
	DataNotFound         error
	ProcessFailed        error
	DuplicatedData       error
	PrimaryKeyDuplicated error
	ServerErr            error
	errorCodeMap         map[pq.ErrorCode]error
)

func init() {
	InitializeFailed = droipkg.NewError("Initialize Failed")
	InvalidParameter = droipkg.NewError("Invalid Parameter")
	ResourceNotFound = droipkg.NewError("Resource Not Found")
	DatabaseUnavailable = droipkg.NewError("Resource Not Found")
	ServerErr = droipkg.NewError("Server Execution Error")
	DataNotFound = droipkg.NewError("Data Not Found")
	ProcessFailed = droipkg.NewError("Processing Failed")
	DuplicatedData = droipkg.NewError("Duplicated Data")
	PrimaryKeyDuplicated = droipkg.NewError("Primary Key Duplicated")
	errorCodeMap = map[pq.ErrorCode]error{
		"42P01": ResourceNotFound,
		"42P07": DuplicatedData,
		"23505": PrimaryKeyDuplicated,
	}
}

func checkDatabaseError(err error, errPtr *error) {
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
