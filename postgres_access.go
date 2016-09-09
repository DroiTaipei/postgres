package postgres

import (
	"github.com/DroiTaipei/droictx"
	"time"
)

func getPg(ctx droictx.Context) (ret *Pg, err error) {
	if pgMode == ONE_NODE_MODE {
		if oneP.Workable() {
			oneP.setCtx(ctx)
			return oneP, nil
		} else {
			return nil, DatabaseUnavailable
		}

	}
	ret, err = rP.RREndPoint()
	if err != nil {
		ret.setCtx(ctx)
	}
	return
}

func OneRecord(ctx droictx.Context, whereClause string, ret interface{}) (err error) {
	p, err := getPg(ctx)
	if err != nil {
		return
	}
	defer sqlLog(ctx, p.DBInfo.Name, whereClause, time.Now())
	checkDatabaseError(p.Conn.First(ret, whereClause).Error, &err)
	return
}

func Query(ctx droictx.Context, where, order string, limit, offset int, ret interface{}) (err error) {

	p, err := getPg(ctx)
	if err != nil {
		return
	}
	q := p.Conn
	if len(where) > 0 {
		q = q.Where(where)
	}
	if len(order) > 0 {
		q = q.Order(order)
	}
	q = q.Offset(offset).Limit(limit)
	defer sqlLog(ctx, p.DBInfo.Name, q.GetSql(&ret), time.Now())

	checkDatabaseError(q.Find(ret).Error, &err)
	return
}

func TableQuery(ctx droictx.Context, table, where, order string, limit, offset int, ret interface{}) (err error) {

	p, err := getPg(ctx)
	if err != nil {
		return
	}
	q := p.Conn.Table(table)
	if len(where) > 0 {
		q = q.Where(where)
	}
	if len(order) > 0 {
		q = q.Order(order)
	}
	q = q.Offset(offset).Limit(limit)
	defer sqlLog(ctx, p.DBInfo.Name, q.GetSql(&ret), time.Now())

	checkDatabaseError(q.Find(ret).Error, &err)
	return
}

func SQLQuery(ctx droictx.Context, querySql string, ret interface{}) (err error) {
	p, err := getPg(ctx)
	if err != nil {
		return
	}
	defer sqlLog(ctx, p.DBInfo.Name, querySql, time.Now())

	checkDatabaseError(p.Conn.Raw(querySql).Scan(ret).Error, &err)

	return
}

func Count(ctx droictx.Context, where string, model interface{}, ret *int) (err error) {

	p, err := getPg(ctx)
	if err != nil {
		return
	}
	q := p.Conn
	if len(where) > 0 {
		q = q.Where(where)
	}
	defer sqlLog(ctx, p.DBInfo.Name, where, time.Now())

	checkDatabaseError(q.Model(model).Count(ret).Error, &err)
	return
}

func Insert(ctx droictx.Context, ret interface{}) (err error) {
	p, err := getPg(ctx)
	if err != nil {
		return
	}
	checkDatabaseError(p.Conn.Create(ret).Error, &err)
	return
}

func OmitInsert(ctx droictx.Context, ret interface{}, omit string) (err error) {
	p, err := getPg(ctx)
	if err != nil {
		return
	}
	checkDatabaseError(p.Conn.Omit(omit).Create(ret).Error, &err)
	return
}

func Update(ctx droictx.Context, ret interface{}, fields map[string]interface{}) (err error) {
	p, err := getPg(ctx)
	if err != nil {
		return
	}

	checkDatabaseError(p.Conn.Model(ret).UpdateColumns(fields).Error, &err)
	return
}

func Delete(ctx droictx.Context, ret interface{}) (err error) {
	p, err := getPg(ctx)
	if err != nil {
		return
	}
	checkDatabaseError(p.Conn.Delete(ret).Error, &err)
	return
}

func Join(ctx droictx.Context, table, fields, join, where, order string, ret interface{}) (err error) {
	p, err := getPg(ctx)
	if err != nil {
		return
	}
	err = p.Conn.
		Table(table).
		Select(fields).
		Joins(join).
		Where(where).
		Order(order).
		Find(ret).Error

	checkDatabaseError(err, &err)
	return
}

func Execute(ctx droictx.Context, sql string, values ...interface{}) (err error) {
	p, err := getPg(ctx)
	if err != nil {
		return
	}
	checkDatabaseError(p.Conn.Exec(sql, values...).Error, &err)
	return
}

func Transaction(ctx droictx.Context, sqls []string) (err error) {
	p, err := getPg(ctx)
	if err != nil {
		return
	}
	tx := p.Conn.Begin()
	b := len(sqls)
	for i := 0; i < b; i++ {
		rawErr := tx.Exec(sqls[i]).Error
		if rawErr != nil {
			tx.Rollback()
			checkDatabaseError(rawErr, &err)
			return
		}
	}
	tx.Commit()
	return
}
