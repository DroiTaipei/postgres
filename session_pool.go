package postgres

import (
	"database/sql"
	"fmt"
	"github.com/DroiTaipei/droictx"
	"github.com/DroiTaipei/droipkg"
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

func (sp *SessionPool) SingelMode(info *DBInfo) {
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

func (sp *SessionPool) RREndPoint() (*Session, droipkg.DroiError) {
	sp.mu.RLock()
	defer sp.mu.RUnlock()
	l := len(sp.validEpList)
	for i := 0; i < l; i++ {
		fmt.Sprintf("%#v", sp.validEpList[i])
	}
	if l == 0 {
		return nil, DatabaseUnavailable
	}
	if l == 1 {
		return sp.validEpList[0], nil
	}
	p := atomic.AddUint64(&sp.pos, 1) - 1

	return sp.validEpList[p%uint64(l)], nil
}

func (sp *SessionPool) getSession(ctx droictx.Context) (ret *Session, err droipkg.DroiError) {
	if sp.mode == SINGLE_MODE {
		if sp.single.Workable() {
			sp.single.setCtx(ctx)
			return sp.single, nil
		} else {
			return nil, DatabaseUnavailable
		}

	}
	ret, err = sp.RREndPoint()
	if err == nil {
		ret.setCtx(ctx)
	}
	return
}

func (sp *SessionPool) OneRecord(ctx droictx.Context, whereClause string, ret interface{}) (err droipkg.DroiError) {
	s, err := sp.getSession(ctx)
	if err != nil {
		return
	}
	return s.OneRecord(ctx, whereClause, ret)
}

func (sp *SessionPool) Query(ctx droictx.Context, where, order string, limit, offset int, ret interface{}) (err droipkg.DroiError) {
	s, err := sp.getSession(ctx)
	if err != nil {
		return
	}
	return s.Query(ctx, where, order, limit, offset, ret)
}

func (sp *SessionPool) TableQuery(ctx droictx.Context, table, where, order string, limit, offset int, ret interface{}) (err droipkg.DroiError) {
	s, err := sp.getSession(ctx)
	if err != nil {
		return
	}
	return s.TableQuery(ctx, table, where, order, limit, offset, ret)
}

func (sp *SessionPool) SQLQuery(ctx droictx.Context, querySql string, ret interface{}) (err droipkg.DroiError) {
	s, err := sp.getSession(ctx)
	if err != nil {
		return
	}
	return s.SQLQuery(ctx, querySql, ret)
}

func (sp *SessionPool) Count(ctx droictx.Context, where string, model interface{}, ret *int) (err droipkg.DroiError) {
	s, err := sp.getSession(ctx)
	if err != nil {
		return
	}
	return s.Count(ctx, where, model, ret)
}

func (sp *SessionPool) Insert(ctx droictx.Context, ret interface{}) (err droipkg.DroiError) {
	s, err := sp.getSession(ctx)
	if err != nil {
		return
	}
	return s.Insert(ctx, ret)
}

func (sp *SessionPool) OmitInsert(ctx droictx.Context, ret interface{}, omit string) (err droipkg.DroiError) {
	s, err := sp.getSession(ctx)
	if err != nil {
		return
	}
	return s.OmitInsert(ctx, ret, omit)
}

func (sp *SessionPool) Update(ctx droictx.Context, ret interface{}, fields map[string]interface{}) (err droipkg.DroiError) {
	s, err := sp.getSession(ctx)
	if err != nil {
		return
	}
	return s.Update(ctx, ret, fields)
}

func (sp *SessionPool) Delete(ctx droictx.Context, ret interface{}) (err droipkg.DroiError) {
	s, err := sp.getSession(ctx)
	if err != nil {
		return
	}
	return s.Delete(ctx, ret)
}

func (sp *SessionPool) Join(ctx droictx.Context, table, fields, join, where, order string, ret interface{}) (err droipkg.DroiError) {
	s, err := sp.getSession(ctx)
	if err != nil {
		return
	}
	return s.Join(ctx, table, fields, join, where, order, ret)
}

func (sp *SessionPool) Execute(ctx droictx.Context, sql string, values ...interface{}) (err droipkg.DroiError) {
	s, err := sp.getSession(ctx)
	if err != nil {
		return
	}
	return s.Execute(ctx, sql, values...)
}

func (sp *SessionPool) Transaction(ctx droictx.Context, sqls []string) (err droipkg.DroiError) {
	s, err := sp.getSession(ctx)
	if err != nil {
		return
	}
	return s.Transaction(ctx, sqls)
}

func (sp *SessionPool) RowScan(ctx droictx.Context, sql string, ptrs ...interface{}) (err droipkg.DroiError) {
	s, err := sp.getSession(ctx)
	if err != nil {
		return
	}
	return s.RowScan(ctx, sql, ptrs...)
}

func (sp *SessionPool) Rows(ctx droictx.Context, sql string) (rows *sql.Rows, err droipkg.DroiError) {
	s, err := sp.getSession(ctx)
	if err != nil {
		return
	}
	return s.Rows(ctx, sql)
}
