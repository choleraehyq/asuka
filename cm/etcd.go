package cm

import (
	"context"
	"path"
	"time"

	"github.com/choleraehyq/asuka/pb/configpb"
	"github.com/coreos/etcd/clientv3"
	"github.com/juju/errors"
	log "github.com/sirupsen/logrus"
)

const (
	kvRangeLimit      = 10000
	kvRequestTimeout  = time.Second * 10
	kvSlowRequestTime = time.Second * 1
)

var (
	errTxnFailed = errors.New("failed to commit transaction")
)

type etcdKVBase struct {
	server *Server
	client *clientv3.Client
}

func newEtcdKVBase(s *Server) *etcdKVBase {
	return &etcdKVBase{
		server: s,
		client: s.client,
	}
}

func (kv *etcdKVBase) Load(key string) (string, error) {
	resp, err := kvGet(kv.server.client, key)
	if err != nil {
		return "", errors.Trace(err)
	}
	if n := len(resp.Kvs); n == 0 {
		return "", nil
	} else if n > 1 {
		return "", errors.Errorf("load more than one kvs: key %v kvs %v", key, n)
	}
	return string(resp.Kvs[0].Value), nil
}

type unmarshaler interface {
	Unmarshal(dAtA []byte) error
}

type marshaler interface {
	Marshal() (dAtA []byte, err error)
}

func (kv *etcdKVBase) LoadProto(key string, unmarshal unmarshaler) error {
	resp, err := kv.Load(key)
	if err != nil {
		return errors.Trace(err)
	}
	if err := unmarshal.Unmarshal([]byte(resp)); err != nil {
		return errors.Trace(err)
	}
	return nil
}

func (kv *etcdKVBase) SaveProto(key string, marshal marshaler) error {
	bv, err := marshal.Marshal()
	if err != nil {
		log.Errorf("marshaler %v marshal failed", marshal)
		return errors.Trace(err)
	}
	if err := kv.Save(key, string(bv)); err != nil {
		return errors.Trace(err)
	}
	return nil
}

func (kv *etcdKVBase) SaveGroup(groupID string, info *configpb.GroupInfo) error {
	return kv.SaveProto(path.Join(groupPathPrefix, groupID), info)
}

func (kv *etcdKVBase) LoadPrefix(prefix string) ([][]byte, error) {
	resp, err := kvGet(kv.server.client, prefix, clientv3.WithPrefix())
	if err != nil {
		return nil, errors.Trace(err)
	}
	res := make([][]byte, 0, len(resp.Kvs))
	for _, item := range resp.Kvs {
		res = append(res, item.Value)
	}
	return res, nil
}

func (kv *etcdKVBase) Save(key string, value string) error {
	resp, err := kv.server.leaderTxn().Then(clientv3.OpPut(key, value)).Commit()
	if err != nil {
		log.Errorf("save kv pair %s->%s to etcd failed: %v", key, value, err)
		return errors.Trace(err)
	}
	if !resp.Succeeded {
		return errors.Trace(errTxnFailed)
	}
	return nil
}

func (kv *etcdKVBase) SaveFreeNode(addr string) error {
	key := path.Join(freeNodePathPrefix, addr)
	if err := kv.Save(key, addr); err != nil {
		log.Errorf("save free node %s to etcd failed: %v", addr, err)
		return errors.Trace(err)
	}
	return nil
}

func (kv *etcdKVBase) Delete(key string) error {
	resp, err := kv.server.leaderTxn().Then(clientv3.OpDelete(key)).Commit()
	if err != nil {
		log.Errorf("delete key %s from etcd failed: %v", key, err)
		return errors.Trace(err)
	}
	if !resp.Succeeded {
		return errors.Trace(errTxnFailed)
	}
	return nil
}

func (kv *etcdKVBase) DeleteFreeNode(addr string) error {
	key := path.Join(freeNodePathPrefix, addr)
	if err := kv.Delete(key); err != nil {
		log.Errorf("delete free node %s from etcd failed: %v", addr, err)
		return errors.Trace(err)
	}
	return nil
}

func (kv *etcdKVBase) DeleteFreeNodes(addrs []string) error {
	for _, addr := range addrs {
		if err := kv.DeleteFreeNode(addr); err != nil {
			return errors.Trace(err)
		}
	}
	return nil
}

func (kv *etcdKVBase) LoadFreeNodes() ([]string, error) {
	bs, err := kv.LoadPrefix(freeNodePathPrefix)
	if err != nil {
		log.Errorf("load free nodes from etcd failed: %v", err)
		return nil, errors.Trace(err)
	}
	ret := make([]string, 0, len(bs))
	for _, v := range bs {
		ret = append(ret, string(v))
	}
	return ret, nil
}

func (kv *etcdKVBase) LoadGroupInfo(groupID string) (*configpb.GroupInfo, error) {
	key := path.Join(groupPathPrefix, groupID)
	ret := &configpb.GroupInfo{}
	if err := kv.LoadProto(key, ret); err != nil {
		log.Errorf("load group info %s failed: %v", groupID)
		return nil, errors.Trace(err)
	}
	return ret, nil
}

func (kv *etcdKVBase) LoadGroupInfos() ([]*configpb.GroupInfo, error) {
	bs, err := kv.LoadPrefix(groupPathPrefix)
	if err != nil {
		log.Errorf("load group infos from etcd failed: %v", err)
		return nil, errors.Trace(err)
	}
	ret := make([]*configpb.GroupInfo, 0, len(bs))
	for _, v := range bs {
		p := &configpb.GroupInfo{}
		if err := p.Unmarshal(v); err != nil {
			log.Errorf("unmarshal groupinfo failed: %v", err)
			return nil, errors.Trace(err)
		}
		ret = append(ret, p)
	}
	return ret, nil
}

func (kv *etcdKVBase) LoadWithPrefix(prefix string) ([]string, error) {
	resp, err := kvGet(kv.server.client, prefix, clientv3.WithPrefix())
	if err != nil {
		log.Errorf("load with prefix %s failed: %v", prefix, err)
		return nil, errors.Trace(err)
	}
	var ret []string
	for _, kv := range resp.Kvs {
		ret = append(ret, string(kv.Value))
	}
	return ret, nil
}

func kvGet(c *clientv3.Client, key string, opts ...clientv3.OpOption) (*clientv3.GetResponse, error) {
	ctx, cancel := context.WithTimeout(c.Ctx(), kvRequestTimeout)
	defer cancel()

	start := time.Now()
	resp, err := clientv3.NewKV(c).Get(ctx, key, opts...)
	if err != nil {
		log.Errorf("load from etcd error: %v", err)
	}
	if cost := time.Since(start); cost > kvSlowRequestTime {
		log.Warnf("kv gets too slow: key %v cost %v err %v", key, cost, err)
	}

	return resp, errors.Trace(err)
}
