package rlock

import (
"fmt"
"io"
"os"
"sync"
"time"
"github.com/coreos/etcd/client"
"golang.org/x/net/context"
"log"
"zdd/app/proto"
"strings"
)

var logger = log.New(os.Stdout,"W",log.Ldate|log.Ltime|log.Llongfile)

const (
	defaultTTL = 5
	defaultTry = 3
	deleteAction = "delete"
	expireAction = "expire"
)

var (
	//ETCD服务器地址，例如127.0.0.1:2357,127.0.0.1:2358,127.0.0.1:2359
	Machine=[]string{}
)

type Mutex struct {
	key    string
	id     string
	client client.Client
	kapi   client.KeysAPI
	ctx    context.Context
	ttl    time.Duration
	mutex  *sync.Mutex
	logger io.Writer
}

func NewEtcLock(tag string, ttl int) sync.Locker {

	if len(tag) == 0  {
		log.Printf("NewEtcLock tag cannot be nil")
		return nil
	}

	if len(Machine) == 0 {
		if len(proto.ETCDServers)==0 {
			logger.Panic("ETCD server IP not found")
		}
		for _,v := range proto.ETCDServers {
			if !strings.HasPrefix( v,"http://") {
				v="http://"+v
			}
			Machine = append(Machine,v)
		}
	}

	cfg := client.Config{
		Endpoints:               Machine,
		Transport:               client.DefaultTransport,
		HeaderTimeoutPerRequest: time.Second,
	}

	c, err := client.New(cfg)
	if err != nil {
		log.Printf("NewEtcLock Create Client Error [%v]", err)
		return nil
	}

	hostname, err := os.Hostname()
	if err != nil {
		log.Printf("NewEtcLock get hostname error [%v]", err)
		return nil
	}

	if tag[0] != '/' {
		tag = "/" + tag
	}

	if ttl < 1 {
		ttl = defaultTTL
	}

	mux := Mutex{
		key:    "/etcdlocker"+tag,
		id:     fmt.Sprintf("%v-%v-%v", hostname, os.Getpid(), time.Now().Format("20060102-15:04:05.999999999")),
		client: c,
		kapi:   client.NewKeysAPI(c),
		ctx: context.TODO(),
		ttl: time.Second * time.Duration(ttl),
		mutex:  new(sync.Mutex),
	}
	return &mux
}

//sync.Lock接口
func (m *Mutex) Lock() {
	m.mutex.Lock()

	succ := make(chan bool)
	notifyErr := make(chan error)
	stop := make(chan bool,1)

	defer close(succ)
	defer close(notifyErr)
	defer close(stop)

	go m.lock(succ,notifyErr,stop)

	select  {
	case <- succ:
		return
	case err:=<-notifyErr:
		if err != nil {
			m.debug("Error get lock [%v]", err)
			panic(err)
		}
	    return
	case <-time.After(time.Second * m.ttl):
		stop <- true
		panic("Get Lock Timeout")
		return
	}
}

func (m *Mutex) lock(succ chan bool,notifyErr chan error,stop chan bool)  {
	//m.debug("Trying to create a node : key=%v", m.key)
	setOptions := &client.SetOptions{
		PrevExist:client.PrevNoExist,
		TTL:      m.ttl,
	}
	//defer close(stop)

	doLock:
	select {
	case <- stop:
		return
	//case <- time.After(time.Millisecond):
	default:

	}

	resp, err := m.kapi.Set(m.ctx, m.key, m.id, setOptions)
	if err == nil {
		//m.debug("Create node %v OK [%q]", m.key, resp)
		succ <- true
		return
	}
	//m.debug("Create node %v failed [%v]", m.key, err)
	e, ok := err.(client.Error)
	if !ok {
		m.debug("Create node %v failed [%v]", m.key, e)
		notifyErr<-e
		return
	}

	if e.Code != client.ErrorCodeNodeExist {
		m.debug("ErrorCodeNodeExist:%v", e)
		notifyErr<-e
		return
	}

	// Get the already node's value.
	resp, err1 := m.kapi.Get(m.ctx, m.key, nil)
	if err1 != nil {
		e, ok := err1.(client.Error)
		if ok && e.Code == client.ErrorCodeKeyNotFound {
			goto doLock
		} else {
			m.debug("Get the already node's value:%v", err1)
			notifyErr <- err1
			return
		}
	}

	//m.debug("Get node %v OK", m.key)
	watcherOptions := &client.WatcherOptions{
		AfterIndex : resp.Index,
		Recursive:false,
	}
	watcher := m.kapi.Watcher(m.key, watcherOptions)
	for {
		//m.debug("Watching %v ...", m.key)
		resp, err = watcher.Next(m.ctx)
		if err != nil {
			e, ok := err.(client.Error)
			if ok && e.Code == client.ErrorCodeKeyNotFound {
				goto doLock
			} else {
				m.debug("Get the already node's value:%v", err)
				notifyErr <- err
				return
			}
		}

		//m.debug("Received an event : %q", resp)
		if resp.Action == deleteAction || resp.Action == expireAction {
			goto doLock
		}
	}

}

func (m *Mutex) Unlock() {
	defer m.mutex.Unlock()
	for i := 1; i <= defaultTry; i++ {
		_, err := m.kapi.Delete(m.ctx, m.key, nil)
		e, ok := err.(client.Error)
		if err != nil {
			if err != nil && ok && e.Code == client.ErrorCodeKeyNotFound{
				return
			}else {
				m.debug("Get the already node's value:%v", err)
			}
		}

	}
	return
}

func (m *Mutex) debug(format string, v ...interface{}) {
	log.Printf(format,v...)
}

func (m *Mutex) SetDebugLogger(w io.Writer) {
	m.logger = w
}
