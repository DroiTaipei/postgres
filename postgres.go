package postgres

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

func Close() {
	if stdPool != nil {
		stdPool.Close()
	}
}
