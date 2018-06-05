package cm

import (
	"context"
	"net/http"
	"time"

	log "github.com/sirupsen/logrus"

	"sync/atomic"

	"github.com/choleraehyq/asuka/pb/metapb"
	"github.com/choleraehyq/asuka/pb/configpb"
	"fmt"
	"github.com/juju/errors"
	"github.com/choleraehyq/asuka/pb/datapb"
	"sort"
)

const (
	freeNodePathPrefix = asukaPathPrefix + "free/"
	groupPathPrefix    = asukaPathPrefix + "group/"
)

func (s *Server) startRPC() {
	defer func() {
		if err := recover(); err != nil {
			log.Errorf("rpc: crash, errors:\n %+v", err)
		}
	}()

	twirpHandler := metapb.NewMetaServiceServer(s, nil)
	mux := http.NewServeMux()
	mux.Handle(metapb.MetaServicePathPrefix, twirpHandler)

	s.rpcServer = &http.Server{
		Addr:    s.cfg.RpcAddr,
		Handler: mux,
	}

	if err := s.rpcServer.ListenAndServe(); err != nil {
		if atomic.LoadInt64(&s.isServing) != 0 {
			log.Fatalf("rpc: ListenAndServe error, listen=<%s> errors:\n %+v",
				s.cfg.RpcAddr,
				err)
			return
		}
	}

	log.Infof("rpc: twirp http server stopped, addr=<%s>", s.cfg.RpcAddr)
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

func (s *Server) sendCreateGroup(addrs []string, info *configpb.GroupInfo) error {
	for _, addr := range addrs {
		client := datapb.NewDataServiceProtobufClient(addr, &http.Client{})
		groupReq := &datapb.GroupChangeReq{
			GroupInfo:info,
			Reason:datapb.CREATE_GROUP,
		}
		// TODO(cholerae): handle error carefully, free node maybe down
		_, err := client.GroupChange(context.Background(), groupReq)
		if err != nil {
			log.Errorf("send group change to %s failed", addr)
			return errors.Trace(err)
		}
	}
	return nil
}

func (s *Server) createGroupInternal(addrs []string, groupID string) (*configpb.GroupInfo, error) {
	log.Debugf("create group from nodes: %v", addrs)
	if len(addrs) < s.cfg.ReplicaNum {
		return nil, errors.New(fmt.Sprintf("not enough addr, replica num: %d, addrs length: %d", s.cfg.ReplicaNum, len(addrs)))
	}
	newGroup := &configpb.GroupInfo{
		GroupInfo: configpb.ConfigNo{
			GroupId: string(groupID),
			Term: 0,
		},
		Primary: addrs[0],
		Secondaries: addrs[1:],
		Learners: nil,
	}
	if err := s.sendCreateGroup(addrs[0:s.cfg.ReplicaNum], newGroup); err != nil {
		return nil, errors.Trace(err)
	}

	if err := s.kv.SaveGroup(groupID, newGroup); err != nil {
		return nil, errors.Trace(err)
	}

	if err := s.kv.DeleteFreeNodes(addrs[0:s.cfg.ReplicaNum]); err != nil {
		return nil, errors.Trace(err)
	}
	return newGroup, nil
}

func (s *Server) GetLatestTerm(ctx context.Context, req *metapb.GetLatestTermReq) (*metapb.GetLatestTermResp, error) {
	groupID := req.Configno.GroupId
	info, err := s.kv.LoadGroupInfo(groupID)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return &metapb.GetLatestTermResp{
		GroupInfo: *info,
	}, nil
}

// return node -> in group number
func mergeGroups(groups []*configpb.GroupInfo) map[string]int {
	ret := make(map[string]int, len(groups))
	for _, group := range groups {
		ret[group.Primary]++
		for _, addr := range group.Secondaries {
			ret[addr]++
		}
		for _, addr := range group.Learners {
			ret[addr]++
		}
	}
	return ret
}

func lowestNodesFromMap(m map[string]int, limit int) []string {
	s := make([]struct{
		Addr string
		Load int
	}, 0, len(m))
	for k, v := range m {
		s = append(s, struct{
			Addr string
			Load int
		}{
			Addr: k,
			Load: v,
		})
	}
	sort.Slice(s, func (i, j int) bool {
		return s[i].Load < s[j].Load
	})
	if limit > len(s) {
		limit = len(s)
	}
	ret := make([]string, 0, limit)
	for i := 0; i < limit; i++ {
		ret = append(ret, s[i].Addr)
	}
	return ret
}

func (s *Server) selectLowLoadedNodesExceptGroups(n int, excepts []*configpb.GroupInfo) ([]string, error) {
	freeNodes, err := s.kv.LoadFreeNodes()
	log.Debugf("free node list: %v", freeNodes)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if len(freeNodes) > n {
		return freeNodes[:n], nil
	}

	groups, err := s.kv.LoadGroupInfos()
	if err != nil {
		return nil, errors.Trace(err)
	}

	for _, except := range excepts {
		for i, group := range groups {
			if group.GroupInfo.GroupId == except.GroupInfo.GroupId {
				groups = append(groups[:i], groups[i+1:]...)
				break
			}
		}
	}

	nodes := mergeGroups(groups)
	if len(nodes) + len(freeNodes) < n {
		log.Warnf("No enough nodes! expect %d nodes, get %d nodes", n, len(nodes)+len(freeNodes))
	}
	chosen := lowestNodesFromMap(nodes, n - len(freeNodes))
	log.Debugf("choose from non-free nodes: %v", chosen)
	return append(freeNodes, chosen...), nil
}

func (s *Server) CreateGroup(ctx context.Context, req *metapb.CreateGroupReq) (*metapb.CreateGroupResp, error) {
	groupID := req.GroupId
	freeNodes, err := s.kv.LoadFreeNodes()
	log.Debugf("free node list: %v", freeNodes)
	if err != nil {
		return nil, errors.Trace(err)
	}

	chosen, err := s.selectLowLoadedNodesExceptGroups(s.cfg.ReplicaNum, nil)
	if err != nil {
		log.Errorf("found %d low loaded nodes failed: %v", s.cfg.ReplicaNum, err)
		return nil, errors.Trace(err)
	}
	log.Debugf("creating new group using %v...", chosen)
	newGroupInfo, err := s.createGroupInternal(chosen, groupID)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return &metapb.CreateGroupResp{
		GroupInfo: *newGroupInfo,
	}, nil
}

func (s *Server) Join(ctx context.Context, req *metapb.JoinReq) (*metapb.JoinResp, error) {
	if err := s.kv.SaveFreeNode(req.Address); err != nil {
		return nil, errors.Annotatef(err, "%s join failed", req.Address)
	}
	ch := make(chan struct{})
	s.heartbeatMu.Lock()
	s.heartbeatChan[req.Address] = ch
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
	}(req.Address, ch)
	return &metapb.JoinResp{}, nil

}

func (s *Server) handleHeartbeatTimeout() {
	for {
		select {
		case <-s.stopC:
			return
		case addr := <-s.heartbeatTimeout:
			s.removeNode(addr)
		}
	}
}

func inStringSlice(x string, slice []string) bool {
	for _, s := range slice {
		if x == s {
			return true
		}
	}
	return false
}

func filterGroupByNode(addr string, infos []*configpb.GroupInfo) []*configpb.GroupInfo {
	ret := make([]*configpb.GroupInfo, 0, len(infos))
	for _, info := range infos {
		if addr == info.Primary || inStringSlice(addr, info.Learners) || inStringSlice(addr, info.Secondaries) {
			ret = append(ret, info)
		}
	}
	return ret
}

func removeStringFromSlice(x string, slice []string) []string {
	for i, s := range slice {
		if x == s {
			return append(slice[:i], slice[i+1:]...)
		}
	}
	return slice
}

func removeNodeAndAddLearner(info *configpb.GroupInfo, addr string, learner string) (*configpb.GroupInfo, error) {
	ret := &configpb.GroupInfo{}
	ret.GroupInfo.GroupId = info.GroupInfo.GroupId
	ret.GroupInfo.Term = info.GroupInfo.Term+1
	if info.Primary == addr {
		if len(info.Secondaries) == 0 {
			log.Errorf("primary and all secondaries of group %s down", info.GroupInfo.GroupId)
			return nil, errors.New("primary and all secondaries down, cannot recover")
		}
		ret.Primary = info.Secondaries[0]
		ret.Secondaries = info.Secondaries[1:]
		ret.Learners = info.Learners
	} else if inStringSlice(addr, info.Secondaries) {
		ret.Primary = info.Primary
		ret.Secondaries = removeStringFromSlice(addr, info.Secondaries)
		ret.Learners = info.Learners
	} else if inStringSlice(addr, info.Learners) {
		ret.Primary = info.Primary
		ret.Secondaries = info.Secondaries
		ret.Learners = removeStringFromSlice(addr, info.Learners)
	} else {
		return nil, errors.New(fmt.Sprintf("No such node %s in this group %s", addr, info.GroupInfo.GroupId))
	}
	// learner == "" means no new learner
	if len(learner) != 0 {
		ret.Learners = append(ret.Learners, learner)
	}
	return ret, nil
}

func (s *Server) removeNode(addr string) error {
	infos, err := s.kv.LoadGroupInfos()
	if err != nil {
		return errors.Trace(err)
	}
	infos = filterGroupByNode(addr, infos)
	for _, info := range infos {
		// Just pick one node to replace down node, so len(nodes) should always be 1
		nodes, err := s.selectLowLoadedNodesExceptGroups(1, infos)
		if err != nil {
			log.Errorf("select 1 low loaded node to replace down node %s failed: %v", addr, err)
			return errors.Trace(err)
		}
		var newGroupInfo *configpb.GroupInfo
		if len(nodes) == 0 {
			// No other available node
			// learner "" means no new learner
			nodes = append(nodes, "")
		}
		// len(nodes) should always be 1, so nodes[0] must be a learner
		newGroupInfo, err = removeNodeAndAddLearner(info, addr, nodes[0])
		if err != nil {
			log.Errorf("remove down node and add learner failed: %v", err)
			return errors.Trace(err)
		}
		if err := s.kv.SaveGroup(newGroupInfo.GroupInfo.GroupId, newGroupInfo); err != nil {
			log.Errorf("persistent new group info failed: %v", err)
			return errors.Trace(err)
		}
		// persistent new group into etcd first and notify data nodes later
		if err := notifyNodesGroupChange(info, newGroupInfo); err != nil {
			return errors.Trace(err)
		}
	}
	return nil
}

func (s *Server) Heartbeat(ctx context.Context,req *metapb.HeartbeatReq) (*metapb.HeartbeatResp, error) {
	s.heartbeatMu.Lock()
	ch, ok := s.heartbeatChan[req.Addr]
	s.heartbeatMu.Unlock()
	if !ok {
		return nil, errors.New("No such node")
	}
	ch <- struct{}{}
	return &metapb.HeartbeatResp{}, nil
}

func (s *Server) QueryLocation(ctx context.Context, req *metapb.QueryLocationReq) (*metapb.QueryLocationResp, error) {
	groupID := req.GroupName
	info, err := s.kv.LoadGroupInfo(groupID)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return &metapb.QueryLocationResp{
		GroupInfo: *info,
	}, nil
}

func upgradeLearner(addr string, info *configpb.GroupInfo) bool {
	for i, v := range info.Learners {
		if v == addr {
			info.Learners = append(info.Learners[:i], info.Learners[i+1:]...)
			info.Secondaries = append(info.Secondaries, addr)
			info.GroupInfo.Term++
			return true
		}
	}
	return false
}

// TODO(cholerae): handle error. Maybe rollback?
func notifyNodesGroupChange(oldInfo *configpb.GroupInfo, info *configpb.GroupInfo) error {
	req := &datapb.GroupChangeReq{
		GroupInfo: info,
	}

	// Send to new learners to let them create instance first.
	for _, addr := range info.Learners {
		client := datapb.NewDataServiceProtobufClient(addr, &http.Client{})
		if !inStringSlice(addr, oldInfo.Learners) {
			req.Reason = datapb.ADD_TO_GROUP
			if _, err := client.GroupChange(context.Background(), req); err != nil {
				log.Errorf("send group change to learner %s failed: %v", addr, err)
				return errors.Trace(err)
			}
		}
	}

	client := datapb.NewDataServiceProtobufClient(info.Primary, &http.Client{})
	req.Reason = datapb.CHANGE_TO_PRIMARY
	if _, err := client.GroupChange(context.Background(), req); err != nil {
		log.Errorf("Send group change to primary %s failed: %v", info.Primary, err)
		return errors.Trace(err)
	}

	for _, addr := range info.Secondaries {
		client := datapb.NewDataServiceProtobufClient(addr, &http.Client{})
		req.Reason = datapb.CHANGE_NODE
		if _, err := client.GroupChange(context.Background(), req); err != nil {
			log.Errorf("send group change to secondary %s failed: %v", addr, err)
			return errors.Trace(err)
		}
	}

	// Only send to old learners
	for _, addr := range info.Learners {
		client := datapb.NewDataServiceProtobufClient(addr, &http.Client{})
		if inStringSlice(addr, oldInfo.Learners) {
			req.Reason = datapb.CHANGE_NODE
			if _, err := client.GroupChange(context.Background(), req); err != nil {
				log.Errorf("send group change to learner %s failed: %v", addr, err)
				return errors.Trace(err)
			}
		}
	}
	return nil
}

func (s *Server) UpgradeLearner(ctx context.Context, req *metapb.UpgradeLearnerReq) (*metapb.UpgradeLearnerResp, error) {
	groupID := req.ConfigNo.GroupId
	info, err := s.kv.LoadGroupInfo(groupID)
	if err != nil {
		return nil, errors.Trace(err)
	}
	oldInfo := *info
	// TODO(cholerae): check term
	if !upgradeLearner(req.Addr, info) {
		return nil, errors.New(fmt.Sprintf("node %s is not a learner of group %s", req.Addr, groupID))
	}
	if err := s.kv.SaveGroup(groupID, info); err != nil {
		log.Errorf("persistent new group info failed: %v", err)
		return nil, errors.Trace(err)
	}
	if err := notifyNodesGroupChange(&oldInfo, info); err != nil {
		return nil, errors.Trace(err)
	}
	return &metapb.UpgradeLearnerResp{}, nil
}
