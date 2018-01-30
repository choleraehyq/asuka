package badger

import (
	"io/ioutil"
	"os"
	"testing"

	"github.com/dgraph-io/badger"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"

	"github.com/choleraehyq/asuka/storage"
)

type BadgerEngineTestSuite struct {
	suite.Suite
	e    storage.Engine
	path string
}

func (suite *BadgerEngineTestSuite) SetupTest() {
	path, err := ioutil.TempDir("/tmp", "asuka-badger-engine-test")
	assert.Nil(suite.T(), err)
	suite.path = path
	suite.e, err = New(suite.path)
	assert.Nil(suite.T(), err)
}

func (suite *BadgerEngineTestSuite) TearDownTest() {
	assert.Nil(suite.T(), suite.e.Close())
	assert.Nil(suite.T(), os.RemoveAll(suite.path))
}

func (suite *BadgerEngineTestSuite) TestBasicKVOps() {
	mustKeyNotExist(suite, "foo")
	assert.Nil(suite.T(), suite.e.Set([]byte("foo"), []byte("bar")))
	mustKeyValueExist(suite, "foo", "bar")
	mustKeyNotExist(suite, "notexist")
	err := suite.e.Delete([]byte("foo"))
	assert.Nil(suite.T(), err)
	mustKeyNotExist(suite, "foo")
}

func (suite *BadgerEngineTestSuite) TestDeleteRange() {
	assert.Nil(suite.T(), suite.e.Set([]byte("foo1"), []byte("bar1")))
	assert.Nil(suite.T(), suite.e.Set([]byte("foo2"), []byte("bar2")))
	assert.Nil(suite.T(), suite.e.Set([]byte("foo3"), []byte("bar3")))
	assert.Nil(suite.T(), suite.e.Set([]byte("foo4"), []byte("bar4")))
	assert.Nil(suite.T(), suite.e.DeleteRange([]byte("foo2"), []byte("")))
	mustKeyValueExist(suite, "foo1", "bar1")
	mustKeyNotExist(suite, "foo2")
	mustKeyNotExist(suite, "foo3")
	mustKeyNotExist(suite, "foo4")

	assert.Nil(suite.T(), suite.e.Set([]byte("foo2"), []byte("bar2")))
	assert.Nil(suite.T(), suite.e.DeleteRange([]byte(""), []byte("")))
	mustKeyNotExist(suite, "foo1")
	mustKeyNotExist(suite, "foo2")

	assert.Nil(suite.T(), suite.e.Set([]byte("foo1"), []byte("bar1")))
	assert.Nil(suite.T(), suite.e.Set([]byte("foo2"), []byte("bar2")))
	assert.Nil(suite.T(), suite.e.Set([]byte("foo3"), []byte("bar3")))
	assert.Nil(suite.T(), suite.e.Set([]byte("foo4"), []byte("bar4")))
	assert.Nil(suite.T(), suite.e.DeleteRange([]byte("foo2"), []byte("foo4")))
	mustKeyValueExist(suite, "foo1", "bar1")
	mustKeyNotExist(suite, "foo2")
	mustKeyNotExist(suite, "foo3")
	mustKeyValueExist(suite, "foo4", "bar4")
}

func (suite *BadgerEngineTestSuite) TestGetTargetSizeKey() {
	assert.Nil(suite.T(), suite.e.Set([]byte("foo1"), []byte("bar1")))
	assert.Nil(suite.T(), suite.e.Set([]byte("foo2"), []byte("bar2")))
	assert.Nil(suite.T(), suite.e.Set([]byte("foo3"), []byte("bar3")))
	assert.Nil(suite.T(), suite.e.Set([]byte("foo4"), []byte("bar4")))
	size, key, err := suite.e.GetTargetSizeKey([]byte(""), []byte(""), 6)
	assert.Equal(suite.T(), uint64(8), size)
	assert.Equal(suite.T(), []byte("foo2"), key)
	assert.Nil(suite.T(), err)
}

func (suite *BadgerEngineTestSuite) TestSnapshot() {
	file, err := ioutil.TempFile("/tmp", "asuka-snapshot")
	assert.Nil(suite.T(), err)
	path := file.Name()
	defer os.Remove(path)
	assert.Nil(suite.T(), file.Close())
	assert.Nil(suite.T(), suite.e.Set([]byte("foo1"), []byte("bar1")))
	assert.Nil(suite.T(), suite.e.Set([]byte("foo2"), []byte("bar2")))
	assert.Nil(suite.T(), suite.e.Set([]byte("foo3"), []byte("bar3")))
	assert.Nil(suite.T(), suite.e.Set([]byte("foo4"), []byte("bar4")))
	assert.Nil(suite.T(), suite.e.CreateSnapshot(path, []byte("foo2"), []byte("foo4")))
	assert.Nil(suite.T(), suite.e.DeleteRange([]byte(""), []byte("")))
	mustKeyNotExist(suite, "foo1")
	mustKeyNotExist(suite, "foo2")
	mustKeyNotExist(suite, "foo3")
	mustKeyNotExist(suite, "foo4")
	assert.Nil(suite.T(), suite.e.ApplySnapshot(path))
	mustKeyNotExist(suite, "foo1")
	mustKeyValueExist(suite, "foo2", "bar2")
	mustKeyValueExist(suite, "foo3", "bar3")
	mustKeyNotExist(suite, "foo4")
}

func mustKeyValueExist(suite *BadgerEngineTestSuite, k, v string) {
	value, err := suite.e.Get([]byte(k))
	assert.Nil(suite.T(), err)
	assert.Equal(suite.T(), []byte(v), value)
}

func mustKeyNotExist(suite *BadgerEngineTestSuite, k string) {
	_, err := suite.e.Get([]byte(k))
	assert.Equal(suite.T(), badger.ErrKeyNotFound, err)
}

func TestBadgerEngineTestSuite(t *testing.T) {
	suite.Run(t, new(BadgerEngineTestSuite))
}
