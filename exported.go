package postgres

import (
	"database/sql"
	"github.com/DroiTaipei/droictx"
	de "github.com/DroiTaipei/droipkg"
	"github.com/devopstaku/gorm"
)

func OneRecord(ctx droictx.Context, ret interface{}, whereClause string, args ...interface{}) (err de.AsDroiError) {
	return stdPool.OneRecord(ctx, ret, whereClause, args...)
}

func Query(ctx droictx.Context, where, order string, limit, offset int, ret interface{}) (err de.AsDroiError) {
	return stdPool.Query(ctx, where, order, limit, offset, ret)
}

func TableQuery(ctx droictx.Context, table, where, order string, limit, offset int, ret interface{}) (err de.AsDroiError) {
	return stdPool.TableQuery(ctx, table, where, order, limit, offset, ret)
}

func SQLQuery(ctx droictx.Context, ret interface{}, querySql string, args ...interface{}) (err de.AsDroiError) {
	return stdPool.SQLQuery(ctx, ret, querySql, args...)
}

func WhereQuery(ctx droictx.Context, where interface{}, order string, limit, offset int, ret interface{}) (err de.AsDroiError) {
	return stdPool.WhereQuery(ctx, where, order, limit, offset, ret)
}

func Count(ctx droictx.Context, where string, model interface{}, ret *int) (err de.AsDroiError) {
	return stdPool.Count(ctx, where, model, ret)
}

func Insert(ctx droictx.Context, ret interface{}) (err de.AsDroiError) {
	return stdPool.Insert(ctx, ret)
}

func OmitInsert(ctx droictx.Context, ret interface{}, omit string) (err de.AsDroiError) {
	return stdPool.OmitInsert(ctx, ret, omit)
}

func Update(ctx droictx.Context, ret interface{}, fields map[string]interface{}) (err de.AsDroiError) {
	return stdPool.Update(ctx, ret, fields)
}

func UpdateNonBlank(ctx droictx.Context, ret interface{}) (err de.AsDroiError) {
	return stdPool.UpdateNonBlank(ctx, ret)
}

func CriteriaUpdate(ctx droictx.Context, ret interface{}, fields map[string]interface{}, criteria string, args ...interface{}) de.AsDroiError {
	return stdPool.CriteriaUpdate(ctx, ret, fields, criteria, args ...)
}

func Delete(ctx droictx.Context, ret interface{}) (err de.AsDroiError) {
	return stdPool.Delete(ctx, ret)
}

func CriteriaDelete(ctx droictx.Context, ret interface{}, criteria string, args ...interface{}) (err de.AsDroiError)  {
	return stdPool.CriteriaDelete(ctx, ret, criteria, args ...)
}


func Join(ctx droictx.Context, ret interface{}, table, fields, join, order, criteria string, args...interface{}) (err de.AsDroiError) {
	return stdPool.Join(ctx, ret, table, fields, join, order, criteria, args...)
}

func Execute(ctx droictx.Context, sql string, values ...interface{}) (err de.AsDroiError) {
	return stdPool.Execute(ctx, sql, values...)
}

func Transaction(ctx droictx.Context, sqls []string) (err de.AsDroiError) {
	return stdPool.Transaction(ctx, sqls)
}

func RowScan(ctx droictx.Context, sql string, ptrs ...interface{}) (err de.AsDroiError) {
	return stdPool.RowScan(ctx, sql, ptrs...)
}

func Rows(ctx droictx.Context, sql string) (rows *sql.Rows, err de.AsDroiError) {
	return stdPool.Rows(ctx, sql)
}

func GetGORM(ctx droictx.Context) (ret *gorm.DB, err de.AsDroiError) {
	return stdPool.GetGORM(ctx)
}

func LogMode(ctx droictx.Context, enable bool) (err de.AsDroiError) {
	return stdPool.LogMode(ctx, enable)
}
