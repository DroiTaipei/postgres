package postgres

import (
	"database/sql"
	"fmt"
	"github.com/DroiTaipei/droictx"
	de "github.com/DroiTaipei/droipkg"
	"github.com/DroiTaipei/droipkg/rdb"
	"github.com/devopstaku/gorm"
	"sync"
	"sync/atomic"
)

type SessionPool struct {
	// hosts are the set of all hosts in the cassandra ring that we know of
	mu          sync.RWMutex
	epList      []*Session
	validEpList []*Session
	pos         uint64
	mode        string
	single      *Session
}

func (sp *SessionPool) SingleMode(info *DBInfo) {
	sp.single = newSession(info)
	sp.mode = SINGLE_MODE
}

func (sp *SessionPool) RoundRobinMode(infos []*DBInfo) {
	b := len(infos)
	for i := 0; i < b; i++ {
		stdPool.AddEndPoint(newSession(infos[i]))
	}
	stdPool.CheckValidList()
	sp.mode = ROUND_ROBIN_MODE
}

func (sp *SessionPool) Initialize(infos []*DBInfo, accessTarget string) error {
	b := len(infos)
	if b == 0 {
		return de.NewError("Initialize Failed: Empty PG DB Infos")
	}
	if accessTarget == ROUND_ROBIN_MODE {
		sp.RoundRobinMode(infos)
		return nil
	} else {
		for i := 0; i < b; i++ {
			if infos[i].Name == accessTarget {
				sp.SingleMode(infos[i])
				return nil
			}
		}

		return de.NewError("Initialize Failed: Invalid Single Mode Config")
	}
	return nil

}

func (sp *SessionPool) Close() {
	switch sp.mode {
	case SINGLE_MODE:
		if sp.single != nil {
			sp.single.Close()
		}
	case ROUND_ROBIN_MODE:
		ss := sp.AllEndPoints()
		b := len(ss)
		for i := 0; i < b; i++ {
			ss[i].Close()
		}
	}
}

func (sp *SessionPool) Reconnect() {
	switch sp.mode {
	case SINGLE_MODE:
		if sp.single != nil {
			sp.single.reconnect()
		}
	// Wait to verified
	case ROUND_ROBIN_MODE:
		ss := sp.AllEndPoints()
		b := len(ss)
		for i := 0; i < b; i++ {
			ss[i].reconnect()
		}
	}

}

func (sp *SessionPool) AllEndPoints() []*Session {
	return sp.validEpList
}

func (sp *SessionPool) AddEndPoint(s *Session) {
	sp.mu.Lock()
	sp.epList = append(sp.epList, s)
	sp.mu.Unlock()
	s.setPool(sp)
}

func (sp *SessionPool) CheckValidList() {
	sp.mu.Lock()
	defer sp.mu.Unlock()
	sp.validEpList = sp.validEpList[:0]
	b := len(sp.epList)
	for i := 0; i < b; i++ {
		if sp.epList[i].Workable() {
			sp.validEpList = append(sp.validEpList, sp.epList[i])
		}
	}
}

func (sp *SessionPool) RREndPoint() (*Session, de.AsDroiError) {
	sp.mu.RLock()
	defer sp.mu.RUnlock()
	l := len(sp.validEpList)
	for i := 0; i < l; i++ {
		fmt.Sprintf("%#v", sp.validEpList[i])
	}
	if l == 0 {
		return nil, de.NewTraceWithMsg(rdb.ErrDatabaseUnavailable,"")
	}
	if l == 1 {
		return sp.validEpList[0], nil
	}
	p := atomic.AddUint64(&sp.pos, 1) - 1

	return sp.validEpList[p%uint64(l)], nil
}

func (sp *SessionPool) getSession(ctx droictx.Context) (ret *Session, err de.AsDroiError) {
	if sp.mode == SINGLE_MODE {
		if sp.single.Workable() {
			sp.single.setCtx(ctx)
			return sp.single, nil
		} else {
			return nil, de.NewTraceWithMsg(rdb.ErrDatabaseUnavailable,"")
		}

	}
	ret, err = sp.RREndPoint()
	if err == nil {
		ret.setCtx(ctx)
	}
	return
}

func (sp *SessionPool) OneRecord(ctx droictx.Context, ret interface{}, whereClause string, args ...interface{}) (err de.AsDroiError) {
	s, err := sp.getSession(ctx)
	if err != nil {
		return
	}
	return s.OneRecord(ctx, ret, whereClause, args...)
}

func (sp *SessionPool) Query(ctx droictx.Context, where, order string, limit, offset int, ret interface{}) (err de.AsDroiError) {
	s, err := sp.getSession(ctx)
	if err != nil {
		return
	}
	return s.Query(ctx, where, order, limit, offset, ret)
}

func (sp *SessionPool) TableQuery(ctx droictx.Context, table, where, order string, limit, offset int, ret interface{}) (err de.AsDroiError) {
	s, err := sp.getSession(ctx)
	if err != nil {
		return
	}
	return s.TableQuery(ctx, table, where, order, limit, offset, ret)
}

func (sp *SessionPool) SQLQuery(ctx droictx.Context, ret interface{}, querySql string, args ...interface{}) (err de.AsDroiError) {
	s, err := sp.getSession(ctx)
	if err != nil {
		return
	}
	return s.SQLQuery(ctx, ret, querySql, args...)
}

func (sp *SessionPool) WhereQuery(ctx droictx.Context, where interface{}, order string, limit, offset int, ret interface{}) (err de.AsDroiError) {
	s, err := sp.getSession(ctx)
	if err != nil {
		return
	}
	return s.WhereQuery(ctx, where, order, limit, offset, ret)
}

func (sp *SessionPool) Count(ctx droictx.Context, where string, model interface{}, ret *int) (err de.AsDroiError) {
	s, err := sp.getSession(ctx)
	if err != nil {
		return
	}
	return s.Count(ctx, where, model, ret)
}

func (sp *SessionPool) Insert(ctx droictx.Context, ret interface{}) (err de.AsDroiError) {
	s, err := sp.getSession(ctx)
	if err != nil {
		return
	}
	return s.Insert(ctx, ret)
}

func (sp *SessionPool) OmitInsert(ctx droictx.Context, ret interface{}, omit string) (err de.AsDroiError) {
	s, err := sp.getSession(ctx)
	if err != nil {
		return
	}
	return s.OmitInsert(ctx, ret, omit)
}

func (sp *SessionPool) Update(ctx droictx.Context, ret interface{}, fields map[string]interface{}) (err de.AsDroiError) {
	s, err := sp.getSession(ctx)
	if err != nil {
		return
	}
	return s.Update(ctx, ret, fields)
}

func (sp *SessionPool) UpdateNonBlank(ctx droictx.Context, ret interface{}) (err de.AsDroiError) {
	s, err := sp.getSession(ctx)
	if err != nil {
		return
	}
	return s.UpdateNonBlank(ctx, ret)
}

func (sp *SessionPool) CriteriaUpdate(ctx droictx.Context, ret interface{}, fields map[string]interface{}, criteria string, args ...interface{}) (err de.AsDroiError)  {
	s, err := sp.getSession(ctx)
	if err != nil {
		return 
	}
	return s.CriteriaUpdate(ctx, ret, fields, criteria, args ...)
}

func (sp *SessionPool) Delete(ctx droictx.Context, ret interface{}) (err de.AsDroiError) {
	s, err := sp.getSession(ctx)
	if err != nil {
		return
	}
	return s.Delete(ctx, ret)
}

func (sp *SessionPool) CriteriaDelete(ctx droictx.Context, ret interface{}, criteria string, args ...interface{}) (err de.AsDroiError)  {
	s, err := sp.getSession(ctx)
	if err != nil {
		return
	}
	return s.CriteriaDelete(ctx, ret, criteria, args ...)
}

func (sp *SessionPool) Join(ctx droictx.Context, ret interface{}, table, fields, join, order, criteria string, args ...interface{}) (err de.AsDroiError) {
	s, err := sp.getSession(ctx)
	if err != nil {
		return
	}
	return s.Join(ctx, ret, table, fields, join, order, criteria, args ...)
}

func (sp *SessionPool) Execute(ctx droictx.Context, sql string, values ...interface{}) (err de.AsDroiError) {
	s, err := sp.getSession(ctx)
	if err != nil {
		return
	}
	return s.Execute(ctx, sql, values...)
}

func (sp *SessionPool) Transaction(ctx droictx.Context, sqls []string) (err de.AsDroiError) {
	s, err := sp.getSession(ctx)
	if err != nil {
		return
	}
	return s.Transaction(ctx, sqls)
}

func (sp *SessionPool) RowScan(ctx droictx.Context, sql string, ptrs ...interface{}) (err de.AsDroiError) {
	s, err := sp.getSession(ctx)
	if err != nil {
		return
	}
	return s.RowScan(ctx, sql, ptrs...)
}

func (sp *SessionPool) Rows(ctx droictx.Context, sql string) (rows *sql.Rows, err de.AsDroiError) {
	s, err := sp.getSession(ctx)
	if err != nil {
		return
	}
	return s.Rows(ctx, sql)
}

func (sp *SessionPool) GetGORM(ctx droictx.Context) (ret *gorm.DB, err de.AsDroiError) {
	s, err := sp.getSession(ctx)
	if err != nil {
		return
	}
	ret = s.Conn.New()
	return
}

//LogMode : For enabling log
func (sp *SessionPool) LogMode(ctx droictx.Context, enable bool) (err de.AsDroiError) {
	// FIXME
	// It should be for all session
	s, err := sp.getSession(ctx)
	if err != nil {
		return
	}
	s.Conn.LogMode(enable)
	return
}
