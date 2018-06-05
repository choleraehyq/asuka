package cm

import (
	"context"
	"time"

	"github.com/coreos/etcd/clientv3"
	log "github.com/sirupsen/logrus"

	"net/http"

	"github.com/choleraehyq/asuka/pb/datapb"
	"github.com/juju/errors"
)

const (
	leaderLeaseTTL     time.Duration = 6
	asukaPathPrefix                  = "/asuka/"
	cmEtcdPathPrefix                 = asukaPathPrefix + "cm/"
	dnEtcdPathPrefix                 = asukaPathPrefix + "dn/"
	leaderElectionPath               = cmEtcdPathPrefix + "election"
)

func (s *Server) startLeaderLoop() {
	s.leaderLoopCtx, s.leaderLoopCancel = context.WithCancel(context.Background())
	s.leaderLoopWg.Add(1)
	go s.leaderLoop()
}

func (s *Server) stopLeaderLoop() {
	s.leaderLoopCancel()
	s.leaderLoopWg.Wait()
}

func (s *Server) leaderLoop() {
	defer s.leaderLoopWg.Done()

	lease, err := s.client.Grant(s.leaderLoopCtx, int64(leaderLeaseTTL))
	s.client.KeepAliveOnce(s.leaderLoopCtx, lease.ID)

	_, err = s.client.Put(s.leaderLoopCtx, cmEtcdPathPrefix+s.cfg.RpcAddr, "http://" + s.cfg.RpcAddr, clientv3.WithLease(lease.ID))
	if err != nil {
		log.Fatalf("register self rpc address failed: %v\n", err)
	}

	for {
		if s.isClosed() {
			log.Infof("server is closed, return leader loop")
			return
		}

		s.client.KeepAliveOnce(s.leaderLoopCtx, lease.ID)

		time.Sleep(leaderLeaseTTL / 2 * time.Second)

		isNewLeader, err := s.compaignLeader(lease.ID)
		if err != nil {
			log.Debugf("campaign leader failed: %v\n", err)
		}
		if isNewLeader {
			dataNodes, err := s.getDataNodes()
			if err != nil {
				log.Errorf("get data node list from etcd failed: %v", err)
				continue
			}
			metaNodes, err := s.getMetaNodes()
			req := &datapb.MetaServerChangeReq{
				Primary:     "http://" + s.cfg.RpcAddr,
				NewMetaList: metaNodes,
			}
			for _, dataNode := range dataNodes {
				client := datapb.NewDataServiceProtobufClient(dataNode, &http.Client{})
				if _, err := client.MetaServerChange(context.Background(), req); err != nil {
					log.Errorf("MetaServerChange data node %s failed: %v", dataNode, err)
				}
				// like join
				ch := make(chan struct{})
				s.heartbeatMu.Lock()
				if _, ok := s.heartbeatChan[dataNode]; ok {
					s.heartbeatMu.Unlock()
					continue
				}
				s.heartbeatChan[dataNode] = ch
				s.heartbeatMu.Unlock()
				go func(addr string, ch <-chan struct{}) {
					for {
						select {
						case <-s.stopC:
							return
						case <-ch:
							break
						case <-time.After(checkHeartbeatInterval):
							log.Errorf("node %s heartbeat timeout!", addr)
							s.heartbeatTimeout <- addr
							return
						}
					}
				}(dataNode, ch)
			}
		}
		leader, err := s.getLeader()
		if err != nil {
			log.Errorf("get leader failed: %v", err)
			continue
		}
		log.Debugf("leader is %s now\n", leader)
	}
}

// true means compaignLeader to new leader
func (s *Server) compaignLeader(lease clientv3.LeaseID) (bool, error) {
	txn := s.txn().If(clientv3.Compare(clientv3.Version(leaderElectionPath), "=", 0)).Then(clientv3.OpPut(leaderElectionPath, s.leaderValue, clientv3.WithLease(lease)))
	txnResp, err := txn.Commit()
	if err != nil {
		return false, errors.Trace(err)
	}
	if txnResp.Succeeded {
		return true, nil
	}
	return false, nil
}

func (s *Server) IsLeader() bool {
	leader, _ := s.getLeader()
	return leader == s.leaderValue
}

func (s *Server) getLeader() (string, error) {
	resp, err := s.client.Get(s.leaderLoopCtx, leaderElectionPath)
	if err != nil {
		return "", errors.Trace(err)
	}
	return string(resp.Kvs[0].Value), nil
}

func (s *Server) getDataNodes() ([]string, error) {
	return s.kv.LoadWithPrefix(dnEtcdPathPrefix)
}

func (s *Server) getMetaNodes() ([]string, error) {
	return s.kv.LoadWithPrefix(cmEtcdPathPrefix)
}

func (s *Server) leaderCmp() clientv3.Cmp {
	return clientv3.Compare(clientv3.Value(leaderElectionPath), "=", s.leaderValue)
}

func (s *Server) txn() clientv3.Txn {
	return s.client.Txn(s.client.Ctx())
}

func (s *Server) leaderTxn(cs ...clientv3.Cmp) clientv3.Txn {
	return s.txn().If(append(cs, s.leaderCmp())...)
}
