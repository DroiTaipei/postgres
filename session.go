package postgres

import (
	"database/sql"
	"fmt"
	"github.com/DroiTaipei/droictx"
	de "github.com/DroiTaipei/droipkg"
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

func (s *Session) OneRecord(ctx droictx.Context, ret interface{}, whereClause string, args ...interface{}) (de.AsDroiError) {
	where := append([]interface{}{whereClause}, args...)
	defer sqlLog(ctx, s.DBInfo.Name, whereClause, time.Now())
	return s.CheckDatabaseError(s.Conn.First(ret, where...).Error)
}

func (s *Session) Query(ctx droictx.Context, where, order string, limit, offset int, ret interface{}) (de.AsDroiError) {
	q := s.Conn
	if len(where) > 0 {
		q = q.Where(where)
	}
	if len(order) > 0 {
		q = q.Order(order)
	}
	q = q.Offset(offset).Limit(limit)
	// TODO: Try To GetSQL
	defer sqlLog(ctx, s.DBInfo.Name, "", time.Now())

	return s.CheckDatabaseError(q.Find(ret).Error)
}

func (s *Session) TableQuery(ctx droictx.Context, table, where, order string, limit, offset int, ret interface{}) (de.AsDroiError) {
	q := s.Conn.Table(table)
	if len(where) > 0 {
		q = q.Where(where)
	}
	if len(order) > 0 {
		q = q.Order(order)
	}
	q = q.Offset(offset).Limit(limit)
	// TODO: Try To GetSQL
	defer sqlLog(ctx, s.DBInfo.Name, "", time.Now())

	return s.CheckDatabaseError(q.Find(ret).Error)
	
}

func (s *Session) SQLQuery(ctx droictx.Context, ret interface{}, querySql string, args ...interface{}) (de.AsDroiError) {
	defer sqlLog(ctx, s.DBInfo.Name, querySql, time.Now())
	return s.CheckDatabaseError(s.Conn.Raw(querySql, args ...).Scan(ret).Error)
}

func (s *Session) WhereQuery(ctx droictx.Context, where interface{}, order string, limit, offset int, ret interface{}) (de.AsDroiError) {
	tmp := s.Conn.Where(where)
	if len(order) > 0 {
		tmp = tmp.Order(order)
	}
	return s.CheckDatabaseError(tmp.Limit(limit).Offset(offset).Find(ret).Error)
	
}

func (s *Session) Count(ctx droictx.Context, where string, model interface{}, ret *int) (de.AsDroiError) {
	q := s.Conn
	if len(where) > 0 {
		q = q.Where(where)
	}
	defer sqlLog(ctx, s.DBInfo.Name, where, time.Now())
	return s.CheckDatabaseError(q.Model(model).Count(ret).Error)
	
}

func (s *Session) Insert(ctx droictx.Context, ret interface{}) (de.AsDroiError) {
	return s.CheckDatabaseError(s.Conn.Create(ret).Error)
}

func (s *Session) OmitInsert(ctx droictx.Context, ret interface{}, omit string) (de.AsDroiError) {
	return s.CheckDatabaseError(s.Conn.Omit(omit).Create(ret).Error)
}

func (s *Session) Update(ctx droictx.Context, ret interface{}, fields map[string]interface{}) (de.AsDroiError) {
	return s.CheckDatabaseError(s.Conn.Model(ret).UpdateColumns(fields).Error)
}

func (s *Session) UpdateNonBlank(ctx droictx.Context, ret interface{}) (de.AsDroiError) {
	 return s.CheckDatabaseError(s.Conn.Model(ret).Update(ret).Error)
}

func (s *Session) CriteriaUpdate(ctx droictx.Context, ret interface{}, fields map[string]interface{}, criteria string, args ...interface{}) de.AsDroiError {
	return s.CheckDatabaseError(s.Conn.Model(ret).Where(criteria, args...).UpdateColumns(fields).Error)
}

func (s *Session) Delete(ctx droictx.Context, ret interface{}) (de.AsDroiError) {
	return s.CheckDatabaseError(s.Conn.Delete(ret).Error)
}

func (s *Session) CriteriaDelete(ctx droictx.Context, ret interface{}, criteria string, args ...interface{}) de.AsDroiError  {
	return s.CheckDatabaseError(s.Conn.Where(criteria, args...).Delete(ret).Error)
}

func (s *Session) Join(ctx droictx.Context, ret interface{}, table, fields, join, order, criteria string, args ...interface{}) (de.AsDroiError) {
	pgErr := s.Conn.
		Table(table).
		Select(fields).
		Joins(join).
		Where(criteria, args...).
		Order(order).
		Find(ret).Error

	return s.CheckDatabaseError(pgErr)
	
}

func (s *Session) Execute(ctx droictx.Context, sql string, values ...interface{}) (de.AsDroiError) {
	return s.CheckDatabaseError(s.Conn.Exec(sql, values...).Error)
	
}

func (s *Session) Transaction(ctx droictx.Context, sqls []string) (de.AsDroiError) {
	tx := s.Conn.Begin()
	b := len(sqls)
	for i := 0; i < b; i++ {
		rawErr := tx.Exec(sqls[i]).Error
		if rawErr != nil {
			tx.Rollback()
			return s.CheckDatabaseError(rawErr)
			
		}
	}
	tx.Commit()
	return nil
}

func (s *Session) RowScan(ctx droictx.Context, sql string, ptrs ...interface{}) (de.AsDroiError) {
	return s.CheckDatabaseError(s.Conn.Raw(sql).Row().Scan(ptrs...))
	
}

func (s *Session) Rows(ctx droictx.Context, sql string) (rows *sql.Rows, err de.AsDroiError) {
	rows, rawErr := s.Conn.Raw(sql).Rows()
	return rows, s.CheckDatabaseError(rawErr)
}
