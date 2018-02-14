package cm

import (
	"context"
	"time"

	"github.com/coreos/etcd/clientv3"
	log "github.com/sirupsen/logrus"

	"github.com/coreos/etcd/clientv3/concurrency"
)

var (
	leaderLeaseTTL           int64 = 1
	cmEtcdPathPrefix               = "/asuka/cm/"
	leaderElectionPathPrefix       = "/asuka/cm/election"
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

	lease, err := s.client.Grant(s.leaderLoopCtx, leaderLeaseTTL)
	if err != nil {
		log.Fatalf("grant leader lease failed: %v\n", err)
	}

	_, err = s.client.Put(s.leaderLoopCtx, cmEtcdPathPrefix+s.cfg.RpcAddr, "", clientv3.WithLease(lease.ID))
	if err != nil {
		log.Fatalf("register self rpc address failed: %v\n", err)
	}

	session, err := concurrency.NewSession(s.client, concurrency.WithTTL(int(leaderLeaseTTL)))
	if err != nil {
		panic(err)
	}
	defer session.Close()

	s.election = concurrency.NewElection(session, leaderElectionPathPrefix)

	for {
		if s.isClosed() {
			log.Infof("server is closed, return leader loop")
			return
		}

		s.client.KeepAliveOnce(s.leaderLoopCtx, lease.ID)

		time.Sleep(500 * time.Millisecond)

		leader, err := s.getLeader()
		if err != nil {
			log.Errorf("getLeader failed: %v\n", err)
		}
		if len(leader) == 0 {
			if err := s.election.Campaign(s.leaderLoopCtx, s.leaderValue); err != nil {
				log.Errorf("campaign leader failed: %v\n", err)
			}
		} else {
			log.Printf("leader is %s now\n", leader)
			if leader == s.leaderValue {
				if err := s.election.Proclaim(context.Background(), s.leaderValue); err != nil {
					log.Errorf("proclaim self leader failed: %v\n", err)
				}
			}
		}
	}
}

func (s *Server) IsLeader() bool {
	leader, _ := s.getLeader()
	return leader == s.leaderValue
}

func (s *Server) getLeader() (string, error) {
	g, err := s.election.Leader(s.leaderLoopCtx)
	if err == concurrency.ErrElectionNoLeader {
		return "", nil
	} else if err != nil {
		return "", err
	} else {
		return string(g.Kvs[0].Value), nil
	}
}
