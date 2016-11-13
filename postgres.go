package postgres

import (
	"github.com/DroiTaipei/droipkg"
)

var (
	stdPool *SessionPool
)

const (
	DB_TYPE_POSTGRES = "pg"
	SINGLE_MODE      = "SINGLE"
	ROUND_ROBIN_MODE = "ROUNDROBIN"
)

func Initialize(infos []*DBInfo, accessTarget string) error {
	stdPool = &SessionPool{}
	b := len(infos)
	if b == 0 {
		return droipkg.Wrap(InitializeFailed, "Empty PG DB Infos")
	}
	if accessTarget == ROUND_ROBIN_MODE {
		stdPool.RoundRobinMode(infos)
		return nil
	} else {
		for i := 0; i < b; i++ {
			if infos[i].Name == accessTarget {
				stdPool.SingelMode(infos[i])
				return nil
			}
		}

		return droipkg.Wrap(InitializeFailed, "Invalid Single Mode Config")
	}
	return nil

}

func Close() {
	if stdPool != nil {
		ss := stdPool.AllEndPoints()
		b := len(ss)
		for i := 0; i < b; i++ {
			ss[i].Close()
		}
	}

}
