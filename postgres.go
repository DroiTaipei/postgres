package postgres

import "github.com/DroiTaipei/droipkg"

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
	return stdPool.Initialize(infos, accessTarget)
}

func ConnectOne(info *DBInfo) error {
	stdPool = &SessionPool{}
	stdPool.SingleMode(info)
	return nil
}

func RoundRobin(infos []*DBInfo) error {
	stdPool = &SessionPool{}
	stdPool.RoundRobinMode(infos)
	return nil
}

func Reconnect() error {
	if stdPool == nil {
		return droipkg.NewError("There is no alived for reconnecting")
	}
	stdPool.Reconnect()
	return nil
}

func Close() {
	if stdPool != nil {
		stdPool.Close()
	}
}
