package main

import (
	"flag"
	"os"
	"os/signal"
	"syscall"

	"github.com/choleraehyq/asuka/cm"
	"github.com/choleraehyq/asuka/utils"
	log "github.com/sirupsen/logrus"
)

var (
	addrRPC       = flag.String("rpc-addr", "127.0.0.1:9876", "addr rpc service listening")
	addrEtcd      = flag.String("etcd-addr", "127.0.0.1:2379", "etcd cluster address, separate with comma")
	replicaNumber = flag.Int("replica-number", 2, "replica number of each PacificA instance")
	logFile       = flag.String("log-file", "", "The external log file. Default log to console.")
	logLevel      = flag.String("log-level", "debug", "The log level")
)

func main() {
	flag.Parse()
	cfg := parseCfg()

	if *logFile != "" {
		file, err := os.Open(*logFile)
		if err != nil {
			log.Fatalf("open log file %s error: %v\n", *logFile, err)
		}
		log.SetOutput(file)
	}

	log.AddHook(&utils.ContextHook{})
	log.SetFormatter(&utils.TextFormatter{})
	lvl, err := log.ParseLevel(*logLevel)
	if err != nil {
		log.Fatalf("log-level %s is invalid level name", *logLevel)
	}
	log.SetLevel(lvl)

	s := cm.NewServer(cfg)

	sc := make(chan os.Signal, 1)
	signal.Notify(sc,
		syscall.SIGINT,
		syscall.SIGTERM,
		syscall.SIGQUIT)

	if err := s.Start(); err != nil {
		log.Fatalf("start server failed: %v\n", err)
	}

	sig := <-sc
	s.Stop()
	log.Infof("exit: signal=<%d>.\n", sig)
	switch sig {
	case syscall.SIGTERM:
		log.Infoln("exit: bye :-).")
		os.Exit(0)
	default:
		log.Infoln("exit: bye :-(.")
		os.Exit(1)
	}
}

func parseCfg() *cm.Cfg {
	if *addrRPC == "" {
		log.Fatalln("ConfigManager rpc addr must be set")
	}

	if *addrEtcd == "" {
		log.Fatalln("Etcd addr must be set")
	}

	return &cm.Cfg{
		RpcAddr:    *addrRPC,
		EtcdAddr:   *addrEtcd,
		ReplicaNum: *replicaNumber,
	}
}
