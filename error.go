package postgres

import (
	"net"
	"github.com/DroiTaipei/droipkg/rdb"
	de "github.com/DroiTaipei/droipkg"
	"github.com/devopstaku/gorm"
	"github.com/lib/pq"
)


var (
	errorCodeMap         map[pq.ErrorCode]de.DroiError
)

func init() {
	errorCodeMap = map[pq.ErrorCode]de.DroiError{
		"42P01": rdb.ErrResourceNotFound,
		"42P07": rdb.ErrDuplicatedData,
		"23505": rdb.ErrPrimaryKeyDuplicated,
		"42601": rdb.ErrProcessFailed,
		"42703": rdb.ErrResourceNotFound,
		"42704": rdb.ErrResourceNotFound,
	}
}

func (s *Session) CheckDatabaseError(err error) (ret de.AsDroiError){
	var dErr de.DroiError
	
	if err != nil {
		dErr = rdb.ErrDatabase
		switch err {
		case gorm.ErrRecordNotFound:
			dErr = rdb.ErrDataNotFound
		case gorm.ErrInvalidSQL:
			dErr = rdb.ErrProcessFailed
		default:
			switch e := err.(type) {
			case *pq.Error:
				m, handled := errorCodeMap[e.Code]
				if handled {
					dErr = m
				} else {
					debug("Unhandle:", e.Code)
				}
			case net.Error:
				dErr = rdb.ErrDatabaseUnavailable
				s.unWorkable()
			}
		}
		return de.NewTraceDroiError(dErr, err) 
	}
	return nil
}
