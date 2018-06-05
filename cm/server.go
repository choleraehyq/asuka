package cm

import (
	"sync/atomic"

	"github.com/juju/errors"

	"context"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/choleraehyq/asuka/dn"
	"github.com/coreos/etcd/clientv3"
	log "github.com/sirupsen/logrus"
)

const (
	etcdDialTimeout        = 3 * time.Second
	checkHeartbeatInterval = 2 * dn.HeartbeatInterval
)

// Server the pd server
type Server struct {
	cfg *Cfg

	isServing int64
	stopC     chan struct{}

	client *clientv3.Client

	// rpc fields
	rpcServer *http.Server
	kv        *etcdKVBase

	// heartbeat timeout node address
	heartbeatTimeout chan string
	heartbeatMu      sync.Mutex
	heartbeatChan    map[string]chan struct{}

	leaderValue      string
	leaderLoopCtx    context.Context
	leaderLoopCancel context.CancelFunc
	leaderLoopWg     sync.WaitGroup
}

// NewServer create a pd server
func NewServer(cfg *Cfg) *Server {
	return &Server{
		cfg:         cfg,
		leaderValue: cfg.RpcAddr,
		stopC:       make(chan struct{}),
		// TODO(cholerae): use a more appropriate size
		heartbeatTimeout: make(chan string, 100),
		heartbeatChan:    make(map[string]chan struct{}),
	}
}

// Start start the pd server
func (s *Server) Start() error {
	go s.handleHeartbeatTimeout()
	go s.startRPC()

	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   strings.Split(s.cfg.EtcdAddr, ","),
		DialTimeout: etcdDialTimeout,
	})
	s.kv = newEtcdKVBase(s)
	if err != nil {
		return errors.Trace(err)
	}
	s.client = cli

	atomic.StoreInt64(&s.isServing, 1)
	s.startLeaderLoop()
	return nil
}

// Stop the server
func (s *Server) Stop() {
	if !atomic.CompareAndSwapInt64(&s.isServing, 1, 0) {
		log.Errorln("server has already been stopped")
		return
	}
	log.Info("stopping server")
	s.stopLeaderLoop()
	s.closeRPC()
	close(s.stopC)
	if s.client != nil {
		if err := s.client.Close(); err != nil {
			log.Errorf("closing etcd client failed: %v\n", err)
		}
	}
}

func (s *Server) isClosed() bool {
	return atomic.LoadInt64(&s.isServing) == 0
}
