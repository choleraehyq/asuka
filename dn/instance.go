package dn

import (
	"github.com/choleraehyq/asuka/storage"
	"sync"
	"github.com/choleraehyq/asuka/pb/configpb"
	"github.com/choleraehyq/asuka/pb/datapb"
	"github.com/juju/errors"
	"fmt"
	"encoding/binary"
)

type instanceStatus int64

const (
	PRIMARY instanceStatus = iota
	SECONDARY
	LEARNER
)

type instance struct {
	engine storage.Engine
	path string

	// prepared list
	preparedSn uint64
	// commit point
	commitSn uint64

	groupId string
	term uint64

	status instanceStatus
	// primary address
	primary string
	// secondaries address
	secondaries []string
	// learners address
	learners []string

	sync.RWMutex

	// whether is a goroutine running syncLog
	flag int32
}

func (i *instance) initFromGroupInfo(info *configpb.GroupInfo, selfAddr string) {
	i.groupId = info.GroupInfo.GroupId
	i.term = info.GroupInfo.Term
	i.primary = info.Primary
	i.secondaries = info.Secondaries
	i.learners = info.Learners
	if selfAddr == i.primary {
		i.status = PRIMARY
		return
	}
	for _, v := range i.secondaries {
		if selfAddr == v {
			i.status = SECONDARY
			return
		}
	}
	for _, v := range i.learners {
		if selfAddr == v {
			i.status = LEARNER
			return
		}
	}
}

func (i *instance) getLogWithoutLock(sn uint64) (*datapb.Log, error) {
	if i.preparedSn < sn {
		return nil, errors.New(fmt.Sprintf("get too high sn log %d, current highest sn %d", sn, i.preparedSn))
	}
	logKey := make([]byte, binary.MaxVarintLen64)
	binary.PutUvarint(logKey, sn)
	logValue, err := i.engine.Get(encodeLogKey(logKey))
	if err != nil {
		return nil, errors.Annotatef(err, "get log sn %d from engine failed", sn)
	}
	l := &datapb.Log{}
	if err := l.Unmarshal(logValue); err != nil {
		return nil, errors.Annotatef(err, "unmarshal log sn %d failed", sn)
	}
	return l, nil
}

func (i *instance) getLog(sn uint64) (*datapb.Log, error) {
	i.Lock()
	defer i.Unlock()
	return i.getLogWithoutLock(sn)
}

func (i *instance) commitLog(sn uint64) error {
	i.Lock()
	defer i.Unlock()
	return i.commitLogWithoutLock(sn)
}

func (i *instance) commitLogWithoutLock(sn uint64) error {
	if i.commitSn+1 != sn || i.preparedSn < sn {
		return errors.New(fmt.Sprintf("invalid commit sn %d, commitSn %d, preparedSn %d", sn, i.commitSn, i.preparedSn))
	}
	l, err := i.getLogWithoutLock(sn)
	if err != nil {
		return errors.Annotatef(err, "commit sn %d failed because get log failed", sn)
	}
	err = i.engine.Set(encodeDataKey(l.Kv.Key), l.Kv.Value)
	if err != nil {
		return errors.Annotatef(err, "commit sn %d failed because engine set failed", sn)
	}
	i.commitSn++
	return nil
}

func (i *instance) appendLog(sn uint64, l *datapb.Log) error {
	i.Lock()
	defer i.Unlock()
	return i.appendLogWithoutLock(sn, l)
}

func (i *instance) appendLogWithoutLock(sn uint64, l *datapb.Log) error {
	if sn != i.preparedSn+1 {
		return errors.New(fmt.Sprintf("invalid append log sn %d, preparedSn %d", sn, i.preparedSn))
	}
	buf := make([]byte, binary.MaxVarintLen64)
	i.preparedSn++
	binary.PutUvarint(buf, i.preparedSn)
	logValue, err := l.Marshal()
	if err != nil {
		i.preparedSn--
		return errors.Annotatef(err, "kv pair %v->%v marshal log failed", l.Kv.Key, l.Kv.Value)
	}
	if err := i.engine.Set(encodeLogKey(buf), logValue); err != nil {
		i.preparedSn--
		return errors.Annotatef(err, "append log sn %d failed", i.preparedSn+1)
	}
	return nil
}
