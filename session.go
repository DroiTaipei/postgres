package postgres

import (
	"database/sql"
	"fmt"
	"github.com/DroiTaipei/droictx"
	"github.com/DroiTaipei/droipkg"
	"github.com/devopstaku/gorm"
	_ "github.com/lib/pq"
	"time"
)

type DBInfo struct {
	MaxConn  int
	MaxIdle  int
	Name     string
	Host     string
	Port     int
	Database string
	User     string
	Password string
	// Health Checking Time Interval
	HCInterval time.Duration
}

type Session struct {
	Conn *gorm.DB
	Type string
	DBInfo
	workable bool
	timer    *time.Timer
	pool     *SessionPool
}

func newSession(dbi *DBInfo) *Session {
	s := &Session{DBInfo: *dbi, Type: DB_TYPE_POSTGRES}
	s.connect()
	return s
}

func (s *Session) setPool(sp *SessionPool) {
	s.pool = sp
}

func (s *Session) getConnection(host string, port int, user, password, database string, maxIdle, maxConn int) (err error) {
	var c *gorm.DB
	conninfo := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=disable",
		host, port, user, password, database)
	maxAttempts := 20
	for attempts := 1; attempts <= maxAttempts; attempts++ {
		c, err = gorm.Open("postgres", conninfo)
		if err == nil {
			break
		}
		fmt.Printf("Round %d, Attemp to Connect with %s Failed, with %s\n", attempts, conninfo, err.Error())
		time.Sleep(time.Duration(attempts) * time.Second)
	}
	if err != nil {
		return
	}

	c.DB().SetMaxIdleConns(maxIdle)
	c.DB().SetMaxOpenConns(maxConn)
	c.Exec("SET TIME ZONE 'UTC';")
	s.Conn = c
	return
}

func (s *Session) connect() bool {
	err := s.getConnection(s.DBInfo.Host, s.DBInfo.Port, s.DBInfo.User, s.DBInfo.Password, s.DBInfo.Database, s.DBInfo.MaxIdle, s.DBInfo.MaxConn)
	if err != nil {
		return false
	}
	s.checkWorkable()
	return s.workable
}

func (s *Session) Close() {
	s.Conn.Close()
}

func (s *Session) reconnect() bool {
	tmp := s.Conn
	defer tmp.Close()
	s.workable = false
	return s.connect()
}

func (s *Session) setCtx(ctx droictx.Context) {
	ctx.Set(DB_TYPE_FIELD, s.Type)
	ctx.Set(DB_HOSTNAME_FIELD, s.Name)
}

func (s *Session) unWorkable() {
	s.workable = false
	s.eventToPool()
	go s.check()
}

func (s *Session) check() {
	// Avoid Repeated Call v.check
	// While timer is not nil, it means there is another goroutine
	if s.timer != nil {
		return
	}

	s.timer = time.NewTimer(s.DBInfo.HCInterval)
	for s.timer != nil {
		select {
		case <-s.timer.C:
			debug(" Checking is ", s.Name, " Workable? ")
			if s.connect() {
				s.timer = nil
				debug(s.Name, " is Workable!")
				return
			} else {
				s.timer = time.NewTimer(s.DBInfo.HCInterval)
			}
		}
	}

}

func (s *Session) enWorkable() {
	s.workable = true
	s.eventToPool()
}

func (s *Session) eventToPool() {
	if s.pool != nil {
		s.pool.CheckValidList()
	}
}

func (s *Session) Workable() bool {
	return s.workable
}

func (s *Session) testConnection() (ret bool) {
	err := s.Conn.DB().Ping()
	if err == nil {
		ret = true
	}
	return
}
func (s *Session) checkWorkable() {
	if s.testConnection() {
		s.enWorkable()
	}
}

func (s *Session) OneRecord(ctx droictx.Context, whereClause string, ret interface{}) (err droipkg.DroiError) {
	defer sqlLog(ctx, s.DBInfo.Name, whereClause, time.Now())
	s.CheckDatabaseError(s.Conn.First(ret, whereClause).Error, &err)
	return
}

func (s *Session) Query(ctx droictx.Context, where, order string, limit, offset int, ret interface{}) (err droipkg.DroiError) {
	q := s.Conn
	if len(where) > 0 {
		q = q.Where(where)
	}
	if len(order) > 0 {
		q = q.Order(order)
	}
	q = q.Offset(offset).Limit(limit)
	defer sqlLog(ctx, s.DBInfo.Name, q.GetSql(&ret), time.Now())

	s.CheckDatabaseError(q.Find(ret).Error, &err)
	return
}

func (s *Session) TableQuery(ctx droictx.Context, table, where, order string, limit, offset int, ret interface{}) (err droipkg.DroiError) {
	q := s.Conn.Table(table)
	if len(where) > 0 {
		q = q.Where(where)
	}
	if len(order) > 0 {
		q = q.Order(order)
	}
	q = q.Offset(offset).Limit(limit)
	defer sqlLog(ctx, s.DBInfo.Name, q.GetSql(&ret), time.Now())

	s.CheckDatabaseError(q.Find(ret).Error, &err)
	return
}

func (s *Session) SQLQuery(ctx droictx.Context, querySql string, ret interface{}) (err droipkg.DroiError) {
	defer sqlLog(ctx, s.DBInfo.Name, querySql, time.Now())
	s.CheckDatabaseError(s.Conn.Raw(querySql).Scan(ret).Error, &err)
	return
}

func (s *Session) Count(ctx droictx.Context, where string, model interface{}, ret *int) (err droipkg.DroiError) {
	q := s.Conn
	if len(where) > 0 {
		q = q.Where(where)
	}
	defer sqlLog(ctx, s.DBInfo.Name, where, time.Now())
	s.CheckDatabaseError(q.Model(model).Count(ret).Error, &err)
	return
}

func (s *Session) Insert(ctx droictx.Context, ret interface{}) (err droipkg.DroiError) {
	s.CheckDatabaseError(s.Conn.Create(ret).Error, &err)
	return
}

func (s *Session) OmitInsert(ctx droictx.Context, ret interface{}, omit string) (err droipkg.DroiError) {
	s.CheckDatabaseError(s.Conn.Omit(omit).Create(ret).Error, &err)
	return
}

func (s *Session) Update(ctx droictx.Context, ret interface{}, fields map[string]interface{}) (err droipkg.DroiError) {
	s.CheckDatabaseError(s.Conn.Model(ret).UpdateColumns(fields).Error, &err)
	return
}

func (s *Session) Delete(ctx droictx.Context, ret interface{}) (err droipkg.DroiError) {
	s.CheckDatabaseError(s.Conn.Delete(ret).Error, &err)
	return
}

func (s *Session) Join(ctx droictx.Context, table, fields, join, where, order string, ret interface{}) (err droipkg.DroiError) {
	pgErr := s.Conn.
		Table(table).
		Select(fields).
		Joins(join).
		Where(where).
		Order(order).
		Find(ret).Error

	s.CheckDatabaseError(pgErr, &err)
	return
}

func (s *Session) Execute(ctx droictx.Context, sql string, values ...interface{}) (err droipkg.DroiError) {
	s.CheckDatabaseError(s.Conn.Exec(sql, values...).Error, &err)
	return
}

func (s *Session) Transaction(ctx droictx.Context, sqls []string) (err droipkg.DroiError) {
	tx := s.Conn.Begin()
	b := len(sqls)
	for i := 0; i < b; i++ {
		rawErr := tx.Exec(sqls[i]).Error
		if rawErr != nil {
			tx.Rollback()
			s.CheckDatabaseError(rawErr, &err)
			return
		}
	}
	tx.Commit()
	return
}

func (s *Session) RowScan(ctx droictx.Context, sql string, ptrs ...interface{}) (err droipkg.DroiError) {
	s.CheckDatabaseError(s.Conn.Raw(sql).Row().Scan(ptrs...), &err)
	return
}

func (s *Session) Rows(ctx droictx.Context, sql string) (rows *sql.Rows, err droipkg.DroiError) {
	rows, rawErr := s.Conn.Raw(sql).Rows()
	s.CheckDatabaseError(rawErr, &err)
	return
}
