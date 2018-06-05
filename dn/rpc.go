// TODO(cholerae): use custom error struct
package dn

import (
	"context"
	"fmt"
	"net/http"
	"sync/atomic"
	"time"

	"github.com/choleraehyq/asuka/pb/configpb"
	"github.com/choleraehyq/asuka/pb/datapb"
	"github.com/choleraehyq/asuka/pb/errorpb"
	"github.com/choleraehyq/asuka/pb/metapb"
	"github.com/choleraehyq/asuka/pb/storagepb"
	"github.com/juju/errors"
	log "github.com/sirupsen/logrus"
)

func (s *Server) startRPC() {
	defer func() {
		if err := recover(); err != nil {
			log.Errorf("rpc: crash, errors:\n %+v", err)
		}
	}()

	twirpHandler := datapb.NewDataServiceServer(s, nil)
	mux := http.NewServeMux()
	mux.Handle(datapb.DataServicePathPrefix, twirpHandler)

	s.rpcServer = &http.Server{
		Addr:    s.cfg.RpcAddr,
		Handler: mux,
	}

	s.cfg.RpcAddr = "http://" + s.cfg.RpcAddr

	go func() {
		if err := s.rpcServer.ListenAndServe(); err != nil {
			if atomic.LoadInt64(&s.isServing) != 0 {
				log.Fatalf("rpc: ListenAndServe error, listen=<%s> errors:\n %+v",
					s.cfg.RpcAddr,
					err)
				return
			}
		}
	}()

	log.Infof("rpc: twirp http server started, addr=<%s>", s.cfg.RpcAddr)
}

func (s *Server) closeRPC() {
	if s.rpcServer != nil {
		ctx, _ := context.WithTimeout(context.Background(), 3*time.Second)
		if err := s.rpcServer.Shutdown(ctx); err != nil {
			log.Errorf("rpc: Shutdown twirp http server error, addr=<%s>, errors:\n %+v",
				s.cfg.RpcAddr,
				err)
		}
	}
}

// do not support secondary read
func (s *Server) Get(ctx context.Context, req *datapb.GetReq) (*datapb.GetResp, error) {
	instance := s.getGroupInstance(req.GroupId)
	if instance == nil {
		return nil, errors.New(fmt.Sprintf("group %s not found", req.GroupId))
	}
	instance.RLock()
	defer instance.RUnlock()
	if instance.status != PRIMARY {
		return nil, errors.New(fmt.Sprintf("not leader, leader is %s", instance.primary))
	}
	// commit at once, so no need to check commit sn
	v, err := instance.engine.Get(encodeDataKey(req.Key))
	return &datapb.GetResp{
		Value: v,
	}, err
}

func (s *Server) Set(ctx context.Context, req *datapb.SetReq) (*datapb.SetResp, error) {
	instance := s.getGroupInstance(req.GroupId)
	if instance == nil {
		log.Errorf("groups %s not found", req.GroupId)
		return nil, errors.New(fmt.Sprintf("group %s not found", req.GroupId))
	}
	instance.Lock()
	// TODO(cholerae): reduce critical area
	defer instance.Unlock()

	if instance.status != PRIMARY {
		return nil, errors.New(fmt.Sprintf("not leader, leader is %s", instance.primary))
	}

	// Append local prepare list
	if err := instance.appendLogWithoutLock(instance.preparedSn+1, &datapb.Log{Kv: &req.Pair}); err != nil {
		return nil, errors.Trace(err)
	}

	// Send append log request to all secondaries and learners
	logReq := &datapb.AppendLogReq{
		GroupNo: configpb.ConfigNo{
			GroupId: instance.groupId,
			Term:    instance.term,
		},
		Sn: instance.preparedSn,
		Log: &datapb.Log{
			Kv: &storagepb.KVPair{
				Key:   req.Pair.Key,
				Value: req.Pair.Value,
			},
		},
		CommitSn: instance.commitSn,
	}
	// only append log on secondaries for simplicity
	// TODO(cholerae): append log on learners
	for _, addr := range instance.secondaries {
		client := datapb.NewDataServiceProtobufClient(addr, &http.Client{})
		if _, err := client.AppendLog(context.Background(), logReq); err != nil {
			instance.preparedSn--
			log.Errorf("append log to %s failed: %v", addr, err)
			return nil, errors.Annotatef(err, "append log to %s failed", addr)
		}
	}

	// commit and apply
	if err := instance.commitLogWithoutLock(instance.preparedSn); err != nil {
		return nil, errors.Trace(err)
	}

	log.Debugf("set key %s value %s successfully", string(req.Pair.Key), string(req.Pair.Value))
	return &datapb.SetResp{}, nil
}

func (s *Server) AppendLog(ctx context.Context, req *datapb.AppendLogReq) (*datapb.AppendLogResp, error) {
	instance := s.getGroupInstance(req.GroupNo.GroupId)
	if instance == nil {
		log.Errorf("groups %s not found", req.GroupNo.GroupId)
		return &datapb.AppendLogResp{
			Error: &errorpb.Error{
				ErrorType: &errorpb.Error_GroupNotFound{
					GroupNotFound: &errorpb.GroupNotFound{
						Groups: req.GroupNo.GroupId,
					},
				},
			},
		}, errors.New(fmt.Sprintf("group %s not found", req.GroupNo.GroupId))
	}
	instance.Lock()
	// TODO(cholerae): reduce critical area
	defer instance.Unlock()
	if instance.term != req.GroupNo.Term {
		return nil, errors.New(fmt.Sprintf("stale epoch %d, current term is %d", req.GroupNo.Term, instance.term))
	}
	// commit to align primary
	if req.CommitSn > instance.commitSn {
		for i := instance.commitSn + 1; i <= req.CommitSn; i++ {
			if err := instance.commitLogWithoutLock(i); err != nil {
				log.Errorf("commit log %d failed: %v", i, err)
				return nil, errors.Trace(err)
			}
		}
	}
	if err := instance.appendLogWithoutLock(req.Sn, req.Log); err != nil {
		return &datapb.AppendLogResp{}, errors.Trace(err)
	}
	log.Debugf("append log %d successfully, key %s, value %s", req.Sn, string(req.Log.Kv.Key), string(req.Log.Kv.Value))
	return &datapb.AppendLogResp{
		Sn: instance.preparedSn + 1,
	}, nil
}

// TODO(cholerae): implement
func (s *Server) GetLatestSnapshot(ctx context.Context, req *datapb.GetLatestSnapshotReq) (*datapb.GetLatestSnapshotResp, error) {
	panic("Snapshot has not been implemented")
	return nil, nil
}

func (s *Server) GetLog(ctx context.Context, req *datapb.GetLogReq) (*datapb.GetLogResp, error) {
	instance := s.getGroupInstance(req.GroupNo.GroupId)
	if instance == nil {
		log.Errorf("groups %s not found", req.GroupNo.GroupId)
		return nil, errors.New(fmt.Sprintf("group %s not found", req.GroupNo.GroupId))
	}
	instance.Lock()
	defer instance.Unlock()
	l, err := instance.getLogWithoutLock(req.Sn)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return &datapb.GetLogResp{
		PrepareSn: instance.preparedSn,
		CommitSn:  instance.commitSn,
		Sn:        req.Sn,
		Log:       l,
	}, nil
}

func (s *Server) syncLog(selfAddr string, ins *instance) {
	if !atomic.CompareAndSwapInt32(&ins.flag, 0, 1) {
		log.Debug("syncLog is already running")
		return
	}
	for {
		ins.RLock()
		req := &datapb.GetLogReq{
			GroupNo: configpb.ConfigNo{
				GroupId: ins.groupId,
				Term:    ins.term,
			},
			Sn: ins.preparedSn + 1,
		}
		// TODO(cholerae): ins.primary may be removed from this group during syncing, handle that
		client := datapb.NewDataServiceProtobufClient(ins.primary, &http.Client{})
		ins.RUnlock()
		resp, err := client.GetLog(context.Background(), req)
		// sync done
		if resp.PrepareSn < req.Sn {
			log.Debugf("get log to %d all done", resp.PrepareSn)
			break
		}
		log.Debugf("get log %d done", req.Sn)
		if err != nil {
			log.Errorf("cannot get log %d from primary node %s: %v", req.Sn, ins.primary, err)
		}
		// TODO(cholerae): get log from secondaries

		if err := ins.appendLog(resp.Sn, resp.Log); err != nil {
			log.Errorf("append log %d failed: %v", resp.Sn, err)
		}
	}
	s.rw.RLock()
	primary := s.primary_meta
	addr := s.cfg.RpcAddr
	s.rw.RUnlock()
	client := metapb.NewMetaServiceProtobufClient(primary, &http.Client{})
	ins.RLock()
	upgradeReq := &metapb.UpgradeLearnerReq{
		Addr: addr,
		ConfigNo: &configpb.ConfigNo{
			GroupId: ins.groupId,
			Term:    ins.term,
		},
	}
	ins.RUnlock()
	// TODO(cholerae): handle error
	if _, err := client.UpgradeLearner(context.Background(), upgradeReq); err != nil {
		log.Errorf("upgradeLearner node %s at cm %s failed: %v", upgradeReq.Addr, primary, err)
	}
	if !atomic.CompareAndSwapInt32(&ins.flag, 1, 0) {
		log.Error("ins.flag has been changed by other goroutine")
	}
	return
}

func (s *Server) GroupChange(ctx context.Context, req *datapb.GroupChangeReq) (*datapb.GroupChangeResp, error) {
	ins := s.getGroupInstance(req.GroupInfo.GroupInfo.GroupId)
	if ins == nil && req.Reason != datapb.CREATE_GROUP && req.Reason != datapb.ADD_TO_GROUP {
		log.Errorf("groups %s not found", req.GroupInfo.GroupInfo.GroupId)
		return nil, errors.New(fmt.Sprintf("group %s not found", req.GroupInfo.GroupInfo.GroupId))
	}
	switch req.Reason {
	case datapb.CHANGE_NODE:
		ins.Lock()
		defer ins.Unlock()
		if ins.term > req.GroupInfo.GroupInfo.Term {
			return nil, errors.New(fmt.Sprintf("wrong term %d, current term %d", req.GroupInfo.GroupInfo.Term, ins.term))
		}
		ins.initFromGroupInfo(req.GroupInfo, s.cfg.RpcAddr)
	case datapb.CREATE_GROUP:
		ins, err := s.createInstance(req.GroupInfo.GroupInfo.GroupId, req.GroupInfo)
		if err != nil {
			return nil, errors.Trace(err)
		}
		// new group no learner
		assert(ins.status != LEARNER)
		log.Debugf("create group %s successfully", req.GroupInfo.GroupInfo.GroupId)
	case datapb.ADD_TO_GROUP:
		_, err := s.createInstance(req.GroupInfo.GroupInfo.GroupId, req.GroupInfo)
		if err != nil {
			return nil, errors.Trace(err)
		}
	case datapb.REMOVE_FROM_GROUP:
		panic("not implemented")
	case datapb.CHANGE_TO_PRIMARY:
		ins.Lock()
		defer ins.Unlock()
		if ins.term > req.GroupInfo.GroupInfo.Term {
			return nil, errors.New(fmt.Sprintf("wrong term %d, current term %d", req.GroupInfo.GroupInfo.Term, ins.term))
		}
		ins.initFromGroupInfo(req.GroupInfo, s.cfg.RpcAddr)
		// commit all uncommitted log
		for i := ins.commitSn + 1; i <= ins.preparedSn; i++ {
			ins.commitLogWithoutLock(i)
		}
		log.Debugf("start reconcile to log %d", ins.preparedSn)
		// reconcile all followers with its own commit sn to truncate their logs
		for _, addr := range append(ins.secondaries, ins.learners...) {
			client := datapb.NewDataServiceProtobufClient(addr, &http.Client{})
			reconcileReq := &datapb.ReconcileReq{
				GroupNo: configpb.ConfigNo{
					GroupId: ins.groupId,
					Term:    ins.term,
				},
				Sn: ins.commitSn,
			}
			if _, err := client.Reconcile(ctx, reconcileReq); err != nil {
				log.Errorf("reconcile node %s failed: %v", addr, err)
				return nil, errors.Trace(err)
			}
			log.Debugf("node %s reconcile to log %d done", addr, ins.preparedSn)
		}
	}
	return &datapb.GroupChangeResp{}, nil
}

func (s *Server) MetaServerChange(ctx context.Context, req *datapb.MetaServerChangeReq) (*datapb.MetaServerChangeResp, error) {
	s.rw.Lock()
	defer s.rw.Unlock()
	s.metas = req.NewMetaList
	s.primary_meta = req.Primary
	log.Debugf("ConfigManager change: primary %s, metas %v", s.primary_meta, s.metas)
	return &datapb.MetaServerChangeResp{
		NewMetaList: s.metas,
	}, nil
}

func (s *Server) Reconcile(ctx context.Context, req *datapb.ReconcileReq) (*datapb.ReconcileResp, error) {
	instance := s.getGroupInstance(req.GroupNo.GroupId)
	if instance == nil {
		// even new learners have already created instance.
		log.Errorf("groups %s not found", req.GroupNo.GroupId)
		return nil, errors.New(fmt.Sprintf("group %s not found", req.GroupNo.GroupId))
	}
	instance.RLock()
	if req.GroupNo.Term <= instance.term {
		log.Errorf("reconcile at wrong term %d, current term %d", req.GroupNo.Term, instance.term)
		return nil, errors.New(fmt.Sprintf("wrong term %d", req.GroupNo.Term))
	}
	if instance.commitSn > req.Sn {
		log.Errorf("truncate committed log to %d, current commit sn %d", req.Sn, instance.commitSn)
		return nil, errors.New(fmt.Sprintf("wrong truncate sn %d, current commit sn %d", req.Sn, instance.commitSn))
	}
	// sync log
	// LEARNER will run syncLog in GroupChange
	if instance.preparedSn < req.Sn && instance.status != LEARNER {
		instance.RUnlock()
		// lock inside syncLog
		s.syncLog(s.cfg.RpcAddr, instance)
	} else {
		instance.RUnlock()
	}
	instance.Lock()
	instance.term = req.GroupNo.Term
	// in most cases, LEARNER's preparedSn should smaller than req.Sn, and they will sync this in their own syncLog
	// but in some edge cases, LEARNER may have synced log higher than req.Sn from some secondaries
	if instance.preparedSn > req.Sn {
		instance.preparedSn = req.Sn
	}
	// LEARNER should do this in its own syncLog
	if instance.status != LEARNER {
		for i := instance.commitSn; i < req.Sn; i++ {
			if err := instance.commitLogWithoutLock(i); err != nil {
				log.Errorf("commit log %d failed: %v", err)
				instance.Unlock()
				return nil, errors.Trace(err)
			}
		}
		instance.commitSn = req.Sn
		instance.Unlock()
	} else {
		instance.Unlock()
		go s.syncLog(s.cfg.RpcAddr, instance)
	}
	return &datapb.ReconcileResp{}, nil
}
