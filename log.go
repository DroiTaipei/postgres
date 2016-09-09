package postgres

import (
	"time"

	"github.com/DroiTaipei/droictx"
	"github.com/DroiTaipei/droipkg"
)

const (
	VERSION                  = "1"
	UNKNOWN_VALUE            = "U"
	ACCESS_LOG_VERSION_FIELD = "A"
	FUNCTION_FIELD           = "fc"
	FUNCTION_ARGS_FIELD      = "fa"
	DB_TYPE_FIELD            = "Dt"
	DB_HOSTNAME_FIELD        = "Dh"
	DB_COMMAND_FIELD         = "Dc"
	DB_COMMAND_TIME_FIELD    = "Dct"
	REQUEST_TIME_FIELD       = "Rt"
)

func SpentTime(t time.Time) int64 {
	d := time.Since(t)
	// 其實正解應該是 int64(math.Ceil(d.Seconds() * 1e3))
	// 不過不想浪費效能來算....直接無條件進位了！
	// 所以千萬不能用這個來作效能評估，只能拿來計價（喂
	return (d.Nanoseconds() / 1e6) + 1
}

func sqlLog(ctx droictx.Context, dbName, sql string, start time.Time) {

	droipkg.GetLogger().WithMap(ctx.Map()).
		WithField(DB_COMMAND_FIELD, sql).
		WithField(DB_HOSTNAME_FIELD, dbName).
		WithField(DB_COMMAND_TIME_FIELD, SpentTime(start)).
		Error("NOERR")
}

func debug(args ...interface{}) {
	droipkg.GetLogger().Debug(args...)
}
