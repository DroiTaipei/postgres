package postgres

import (
	"database/sql"
	"github.com/DroiTaipei/droictx"
	"github.com/DroiTaipei/droipkg"
	"github.com/devopstaku/gorm"
)

func OneRecord(ctx droictx.Context, whereClause string, ret interface{}) (err droipkg.DroiError) {
	return stdPool.OneRecord(ctx, whereClause, ret)
}

func Query(ctx droictx.Context, where, order string, limit, offset int, ret interface{}) (err droipkg.DroiError) {
	return stdPool.Query(ctx, where, order, limit, offset, ret)
}

func TableQuery(ctx droictx.Context, table, where, order string, limit, offset int, ret interface{}) (err droipkg.DroiError) {
	return stdPool.TableQuery(ctx, table, where, order, limit, offset, ret)
}

func SQLQuery(ctx droictx.Context, querySql string, ret interface{}) (err droipkg.DroiError) {
	return stdPool.SQLQuery(ctx, querySql, ret)
}

func Count(ctx droictx.Context, where string, model interface{}, ret *int) (err droipkg.DroiError) {
	return stdPool.Count(ctx, where, model, ret)
}

func Insert(ctx droictx.Context, ret interface{}) (err droipkg.DroiError) {
	return stdPool.Insert(ctx, ret)
}

func OmitInsert(ctx droictx.Context, ret interface{}, omit string) (err droipkg.DroiError) {
	return stdPool.OmitInsert(ctx, ret, omit)
}

func Update(ctx droictx.Context, ret interface{}, fields map[string]interface{}) (err droipkg.DroiError) {
	return stdPool.Update(ctx, ret, fields)
}

func Delete(ctx droictx.Context, ret interface{}) (err droipkg.DroiError) {
	return stdPool.Delete(ctx, ret)
}

func Join(ctx droictx.Context, table, fields, join, where, order string, ret interface{}) (err droipkg.DroiError) {
	return stdPool.Join(ctx, table, fields, join, where, order, ret)
}

func Execute(ctx droictx.Context, sql string, values ...interface{}) (err droipkg.DroiError) {
	return stdPool.Execute(ctx, sql, values...)
}

func Transaction(ctx droictx.Context, sqls []string) (err droipkg.DroiError) {
	return stdPool.Transaction(ctx, sqls)
}

func RowScan(ctx droictx.Context, sql string, ptrs ...interface{}) (err droipkg.DroiError) {
	return stdPool.RowScan(ctx, sql, ptrs...)
}

func Rows(ctx droictx.Context, sql string) (rows *sql.Rows, err droipkg.DroiError) {
	return stdPool.Rows(ctx, sql)
}

func GetGORM(ctx droictx.Context) (ret *gorm.DB, err droipkg.DroiError) {
	return stdPool.GetGORM(ctx)
}
