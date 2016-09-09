package postgres

import (
	"fmt"
	"github.com/DroiTaipei/droictx"
	"github.com/DroiTaipei/droipkg"
	"github.com/devopstaku/gorm"
	_ "github.com/lib/pq"
	"time"
)

const (
	DB_TYPE_POSTGRES = "pg"
	ONE_NODE_MODE    = "ONE"
	ROUND_ROBIN_MODE = "ROUNDROBIN"
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

var pgMode string

var oneP *Pg
var rP *ringPg

type Pg struct {
	Conn *gorm.DB
	Type string
	DBInfo
	workable bool
	timer    *time.Timer
}

func Initialize(infos []*DBInfo, accessTarget string) error {
	rP = &ringPg{}
	b := len(infos)
	if b == 0 {
		return droipkg.Wrap(InitializeFailed, "Empty PG DB Infos")
	}
	for i := 0; i < b; i++ {
		rP.AddEndPoint(newPg(infos[i]))
	}
	rP.CheckValidList()
	if accessTarget == ROUND_ROBIN_MODE {
		pgMode = ROUND_ROBIN_MODE
	} else {
		ps := rP.AllEndPoints()
		b = len(ps)
		for i := 0; i < b; i++ {
			if ps[i].DBInfo.Name == accessTarget {
				oneP = ps[i]
			}
		}

		if oneP == nil {
			return droipkg.Wrap(InitializeFailed, "Invalid Config")
		}
		pgMode = ONE_NODE_MODE
	}
	return nil

}

func Close() {
	if rP != nil {
		ps := rP.AllEndPoints()
		b := len(ps)
		for i := 0; i < b; i++ {
			ps[i].Close()
		}
	}

}

func newPg(dbi *DBInfo) *Pg {
	r := &Pg{DBInfo: *dbi, Type: DB_TYPE_POSTGRES}
	r.connect()
	return r
}

func (p *Pg) getConnection(host string, port int, user, password, database string, maxIdle, maxConn int) *gorm.DB {
	var dbError error
	var c *gorm.DB
	conninfo := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=disable",
		host, port, user, password, database)
	maxAttempts := 20
	for attempts := 1; attempts <= maxAttempts; attempts++ {
		c, dbError = gorm.Open("postgres", conninfo)
		if dbError == nil {
			break
		}
		fmt.Println(dbError)
		time.Sleep(time.Duration(attempts) * time.Second)
	}
	if dbError != nil {
		panic(dbError)
	}

	c.DB().SetMaxIdleConns(maxIdle)
	c.DB().SetMaxOpenConns(maxConn)
	c.Exec("SET TIME ZONE 'UTC';")
	return c
}

func (p *Pg) connect() bool {

	p.Conn = p.getConnection(p.DBInfo.Host, p.DBInfo.Port, p.DBInfo.User, p.DBInfo.Password, p.DBInfo.Database, p.DBInfo.MaxIdle, p.DBInfo.MaxConn)
	p.checkWorkable()
	return p.workable
}

func (p *Pg) Close() {
	p.Conn.Close()
}

func (p *Pg) setCtx(ctx droictx.Context) {
	ctx.Set(DB_TYPE_FIELD, p.Type)
	ctx.Set(DB_HOSTNAME_FIELD, p.Name)
}

func (p *Pg) unWorkable() {
	p.workable = false
	rP.CheckValidList()
	go p.check()
}

func (p *Pg) check() {
	// Avoid Repeated Call v.check
	// While timer is not nil, it means there is another goroutine
	if p.timer != nil {
		return
	}

	p.timer = time.NewTimer(p.DBInfo.HCInterval)
	for p.timer != nil {
		select {
		case <-p.timer.C:
			debug(" Checking is ", p.Name, " Workable? ")
			if p.connect() {
				p.timer = nil
				debug(p.Name, " is Workable!")
				return
			} else {
				p.timer = time.NewTimer(p.DBInfo.HCInterval)
			}
		}
	}

}

func (p *Pg) enWorkable() {
	p.workable = true
	rP.CheckValidList()
}

func (p *Pg) Workable() bool {
	return p.workable
}

func (p *Pg) testConnection() (ret bool) {
	err := p.Conn.DB().Ping()
	if err == nil {
		ret = true
	}
	return
}
func (p *Pg) checkWorkable() {
	if p.testConnection() {
		p.enWorkable()
	}
}
