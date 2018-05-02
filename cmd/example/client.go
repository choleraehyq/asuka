package main

import (
	"flag"
	"log"
	"github.com/juju/errors"
	"fmt"
	"github.com/choleraehyq/asuka/pb/metapb"
	"net/http"
	"context"
	"github.com/choleraehyq/asuka/pb/datapb"
	"github.com/choleraehyq/asuka/pb/storagepb"
)

var (
	cmaddr = flag.String("cmaddr", "http://127.0.0.1:9876", "primary config manager listening port")
	group = flag.String("group", "", "group name")
	key = flag.String("key", "", "set or get key")
	value = flag.String("value", "", "set value")
	method = flag.String("method", "", "want do you want to do: set, get, create_group")
)

func checkFlag() error {
	if len(*group) == 0 {
		return errors.New(fmt.Sprintf("invalid group %s", *group))
	}
	switch *method {
	case "get":
		if len(*key) == 0 {
			return errors.New(fmt.Sprintf("invalid key %s", *key))
		}
	case "set":
		if len(*key) == 0 {
			return errors.New(fmt.Sprintf("invalid key %s", *key))
		}
		if len(*value) == 0 {
			return errors.New(fmt.Sprintf("invalid value %s", *value))
		}
	case "create_group":
		break
	default:
		return errors.New(fmt.Sprintf("invalid method %s", *method))
	}
	return nil
}

func getLocationAndQuery() error {
	cmClient := metapb.NewMetaServiceProtobufClient(*cmaddr, &http.Client{})
	locationReq := &metapb.QueryLocationReq{
		GroupName: *group,
	}
	locationResp, err := cmClient.QueryLocation(context.Background(), locationReq)
	if err != nil {
		log.Printf("query location for group %s failed: %v", *group, err)
		return err
	}
	dnClient := datapb.NewDataServiceProtobufClient(locationResp.GroupInfo.Primary, &http.Client{})
	if *method == "set" {
		req := &datapb.SetReq{
			GroupId: *group,
			Pair: storagepb.KVPair{
				Key: []byte(*key),
				Value: []byte(*value),
			},
 		}
 		if _, err := dnClient.Set(context.Background(), req); err != nil {
 			log.Printf("set key %s value %s in group %s failed: %v", *key, *value, *group, err)
 			return err
		}
		log.Printf("set key %s value %s in group %s successfully", *key, *value, *group)
	} else {
		// get
		req := &datapb.GetReq{
			GroupId: *group,
			Key: []byte(*key),
		}
		resp, err := dnClient.Get(context.Background(), req)
		if err != nil {
			log.Printf("get key %s in group %s failed: %v", *key, *group, err)
			return err
		}
		log.Printf("get key %s in group %s successfully: value is %s", *key, *group, string(resp.Value))
	}
	return nil
}

func main() {
	flag.Parse()
	if err := checkFlag(); err != nil {
		log.Fatalf("invalid arguments: %v", err)
		return
	}
	switch *method {
	case "create_group":
		cmClient := metapb.NewMetaServiceProtobufClient(*cmaddr, &http.Client{})
		req := &metapb.CreateGroupReq{
			GroupId: *group,
		}
		if _, err := cmClient.CreateGroup(context.Background(), req); err != nil {
			log.Fatalf("create group %s failed, cmaddr %s: %v", *group, *cmaddr, err)
			return
		}
		log.Printf("create group %s successfully", *group)
	default:
		if err := getLocationAndQuery(); err != nil {
			log.Fatalf("failed: %v", err)
		}
	}
}
