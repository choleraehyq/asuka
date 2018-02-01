package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	server "github.com/choleraehyq/asuka/metaserver"
	"github.com/sirupsen/logrus"
)

var (
	name                        = flag.String("name", "", "Metaserver instance name")
	dataPath                    = flag.String("data", "", "The data dir")
	addrRPC                     = flag.String("addr-rpc", "", "Addr: RPC")
	limitReplicas               = flag.Int("limit-replicas", 3, "Limit: Count of peer replicas")
	limitSnapshots              = flag.Uint64("limit-snapshots", 3, "Limit: If the snapshot count of one store is greater than this value,it will never be used as a source or target store")
	limitStoreDownSec           = flag.Int("interval-store-down", 3600, "Interval(sec): Max store down")
	thresholdStorageRate        = flag.Int("threshold-rate-storage", 80, "Threshold: Max storage rate of used for schduler")
	thresholdPauseWatcher       = flag.Int("threshold-pause-watcher", 10, "Threshold: Pause watcher, after N heartbeat times")
	intervalLeaderLeaseSec      = flag.Int64("interval-leader-lease", 5, "Interval(sec): PD leader lease")
	intervalHeartbeatWatcherSec = flag.Int("interval-heartbeat-watcher", 5, "Interval(sec): Watcher heartbeat")
	urlsClient                  = flag.String("urls-client", "http://127.0.0.1:2371", "URLS: embed etcd client urls")
	urlsAdvertiseClient         = flag.String("urls-advertise-client", "", "URLS(advertise): embed etcd client urls")
	urlsPeer                    = flag.String("urls-peer", "http://127.0.0.1:2381", "URLS: embed etcd peer urls")
	urlsAdvertisePeer           = flag.String("urls-advertise-peer", "", "URLS(advertise): embed etcd peer urls")
	initialCluster              = flag.String("initial-cluster", "", "Initial: embed etcd initial cluster")
	initialClusterState         = flag.String("initial-cluster-state", "new", "Initial: embed etcd initial cluster state")
	logFile                     = flag.String("log-file", "", "The external log file. Default log to console.")
	logLevel                    = flag.String("log-level", "info", "The log level, default is info")
)

func main() {
	flag.Parse()
	cfg := parseCfg()

	if *logFile != "" {
		file, err := os.Open(*logFile)
		if err != nil {
			logrus.Fatalf("open log file %s error: %v\n", *logFile, err)
		}
		defer func() {
			err := file.Close()
			if err != nil {
				log.Fatalf("close log file %s error: %v\n", *logFile, err)
			}
		}()
		logrus.SetOutput(file)
	}

	lvl, err := logrus.ParseLevel(*logLevel)
	if err != nil {
		logrus.Fatalf("log-level %s is invalid level name", *logLevel)
	}
	logrus.SetLevel(lvl)

	s := server.NewServer(cfg)

	sc := make(chan os.Signal, 1)
	signal.Notify(sc,
		syscall.SIGINT,
		syscall.SIGTERM,
		syscall.SIGQUIT)

	go s.Start()

	sig := <-sc
	s.Stop()
	logrus.Infof("exit: signal=<%d>.", sig)
	switch sig {
	case syscall.SIGTERM:
		logrus.Infof("exit: bye :-).")
		os.Exit(0)
	default:
		logrus.Infof("exit: bye :-(.")
		os.Exit(1)
	}
}

func parseCfg() *server.Cfg {
	if *name == "" {
		fmt.Println("Metaserver name must be set")
		os.Exit(-1)
	}

	if *dataPath == "" {
		fmt.Println("Metaserver data path must be set")
		os.Exit(-1)
	}

	if *addrRPC == "" {
		fmt.Println("Metaserver rpc addr must be set")
		os.Exit(-1)
	}

	if *urlsPeer == "" {
		fmt.Println("Metaserver embed etcd peer urls must be set")
		os.Exit(-1)
	}

	if *initialCluster == "" {
		fmt.Println("Metaserver embed etcd initial cluster must be set")
		os.Exit(-1)
	}

	cfg := &server.Cfg{}
	cfg.Name = *name
	cfg.DataPath = *dataPath
	cfg.AddrRPC = *addrRPC
	cfg.DurationLeaderLease = *intervalLeaderLeaseSec
	cfg.DurationHeartbeatWatcher = time.Second * time.Duration(*intervalHeartbeatWatcherSec)
	cfg.ThresholdPauseWatcher = *thresholdPauseWatcher
	cfg.URLsClient = *urlsClient
	cfg.URLsAdvertiseClient = *urlsAdvertiseClient
	cfg.URLsPeer = *urlsPeer
	cfg.URLsAdvertisePeer = *urlsAdvertisePeer
	cfg.InitialCluster = *initialCluster
	cfg.InitialClusterState = *initialClusterState
	cfg.LabelsLocation = strings.Split(*labelsLocation, ",")
	cfg.LimitReplicas = uint32(*limitReplicas)
	cfg.LimitSnapshots = *limitSnapshots
	cfg.LimitStoreDownDuration = time.Second * time.Duration(*limitStoreDownSec)
	cfg.LimitScheduleLeader = *limitScheduleLeader
	cfg.LimitScheduleCell = *limitScheduleCell
	cfg.LimitScheduleReplica = *limitScheduleReplica
	cfg.ThresholdStorageRate = *thresholdStorageRate

	return cfg
}
