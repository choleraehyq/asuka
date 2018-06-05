package dn

import (
	"net/http"
	"sync/atomic"
	log "github.com/sirupsen/logrus"
	"github.com/choleraehyq/asuka/pb/metapb"
	"context"
	"time"
	"sync"
	"path"
	"github.com/choleraehyq/asuka/storage/badger"
	"github.com/juju/errors"
	"github.com/choleraehyq/asuka/pb/configpb"
	"os"
)

const (
	HeartbeatInterval = 200 * time.Millisecond
)

type Server struct {
	cfg *Cfg

	rpcServer *http.Server

	isServing int64

	stopC chan struct{}

	// group_id -> pacificA instance
	instances map[string]*instance

	// metaserver addresses
	metas []string
	primary_meta string
	rw sync.RWMutex
}

// NewServer create a pd server
func NewServer(cfg *Cfg) *Server {
	return &Server{
		cfg:         cfg,
		isServing: 0,
		stopC: make(chan struct{}),
		instances: make(map[string]*instance),
		primary_meta: cfg.CMAddr,
	}
}

func (s *Server) joinCluster() error {
	client := metapb.NewMetaServiceProtobufClient(s.primary_meta, &http.Client{})
	req := &metapb.JoinReq{
		Address: s.cfg.RpcAddr,
	}
	if _, err := client.Join(context.Background(), req); err != nil {
		log.Errorf("join cluster at primary config manager %s failed: %v", s.primary_meta, err)
		return errors.Trace(err)
	}
	log.Infof("node %s join cluster at primary config manager %s successfully", s.cfg.RpcAddr, s.primary_meta)
	return nil
}

func (s *Server) Start() error {
	s.startRPC()

	if err := s.joinCluster(); err != nil {
		return errors.Trace(err)
	}

	go func () {
		ticker := time.NewTicker(HeartbeatInterval)
		defer ticker.Stop()
		for {
			select {
			case <-s.stopC:
				return
			case <-ticker.C:
				s.heartbeat()
			}
		}
	}()

	atomic.StoreInt64(&s.isServing, 1)
	return nil
}

func (s *Server) Stop() {
	if !atomic.CompareAndSwapInt64(&s.isServing, 1, 0) {
		log.Errorln("server has already been stopped")
		return
	}
	s.stopC <- struct{}{}
	s.closeRPC()
	for _, v := range s.instances {
		if err := v.engine.Close(); err != nil {
			log.Errorf("close instance %s engine failed, path %s: %v", v.groupId, v.path, err)
		}
		log.Debugf("close instance %s engine success, path %s", v.groupId, v.path)
	}
}

func (s *Server) heartbeat() {
	s.rw.RLock()
	primary := s.primary_meta
	addr := s.cfg.RpcAddr
	s.rw.RUnlock()
	client := metapb.NewMetaServiceProtobufClient(primary, &http.Client{})
	req := &metapb.HeartbeatReq{
		Addr: addr,
	}
	_, err := client.Heartbeat(context.Background(), req)
	if err != nil {
		log.Errorf("heartbeat to primary config manager %s failed: %v", s.primary_meta, err)
		s.rw.RLock()
		metas := s.metas[:]
		s.rw.RUnlock()
		for _, addr := range metas {
			client := metapb.NewMetaServiceProtobufClient(addr, &http.Client{})
			_, err = client.Heartbeat(context.Background(), req)
			if err != nil {
				log.Errorf("heartbeat to config manager %s failed: %v", addr, err)
			}
			break
		}
	}
}

func (s *Server) getGroupInstance(groupId string) *instance {
	s.rw.RLock()
	ins, ok := s.instances[groupId]
	if !ok {
		s.rw.RUnlock()
		return nil
	}
	s.rw.RUnlock()
	return ins
}

func (s *Server) createInstance(groupID string, info *configpb.GroupInfo) (*instance, error) {
	s.rw.Lock()
	defer s.rw.Unlock()
	dir := s.cfg.DataDir
	dbpath := path.Join(dir, s.cfg.RpcAddr + groupID)
	if err := os.MkdirAll(dbpath, 0777); err != nil {
		return nil, errors.Annotatef(err, "create dbpath %s failed", dbpath)
	}
	eng, err := badger.New(dbpath)
	if err != nil {
		return nil, errors.Annotatef(err, "create engine at path %s failed", dbpath)
	}
	ins := &instance{
		engine : eng,
		path: dbpath,
	}
	ins.initFromGroupInfo(info, s.cfg.RpcAddr)
	// if creating new group, the term of new instance should be 0
	// if reconciling, the term will be set in reconciling
	ins.term = 0
	s.instances[groupID] = ins
	return ins, nil
}