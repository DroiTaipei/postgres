package postgres

import (
	"sync"
	"sync/atomic"
)

type ringPg struct {
	// hosts are the set of all hosts in the cassandra ring that we know of
	mu          sync.RWMutex
	epList      []*Pg
	validEpList []*Pg
	pos         uint64
}

func (r *ringPg) AllEndPoints() []*Pg {
	return r.validEpList
}

func (r *ringPg) AddEndPoint(p *Pg) {
	r.mu.Lock()
	r.epList = append(r.epList, p)
	r.mu.Unlock()
}

func (r *ringPg) CheckValidList() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.validEpList = r.validEpList[:0]
	b := len(r.epList)
	for i := 0; i < b; i++ {
		if r.epList[i].Workable() {
			r.validEpList = append(r.validEpList, r.epList[i])
		}
	}
}

func (r *ringPg) RREndPoint() (*Pg, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	l := len(r.validEpList)
	if l == 0 {
		return nil, DatabaseUnavailable
	}
	if l == 1 {
		return r.validEpList[0], nil
	}
	p := atomic.AddUint64(&r.pos, 1) - 1

	return r.validEpList[p%uint64(l)], nil
}
