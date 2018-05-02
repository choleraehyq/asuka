package main

import (
	log "github.com/sirupsen/logrus"
	"github.com/choleraehyq/asuka/dn"
	"os"
	"os/signal"
	"flag"
	"syscall"
	"path"
	"github.com/choleraehyq/asuka/utils"
)

var (
	addrRPC     = flag.String("rpc-addr", "127.0.0.1:5432", "addr rpc service listening")
	logFile     = flag.String("log-file", "", "The external log file. Default log to console.")
	logLevel    = flag.String("log-level", "debug", "The log level")
	storagePath = flag.String("path", "./asuka.db", "storage file directory path")
	primaryCM   = flag.String("cm", "127.0.0.1:9876", "config manager listening port")
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

	s := dn.NewServer(cfg)

	sc := make(chan os.Signal, 1)
	signal.Notify(sc,
		syscall.SIGINT,
		syscall.SIGTERM,
		syscall.SIGQUIT)

	if err := s.Start(); err != nil {
		log.Errorf("start server at %s failed: %v", *addrRPC, err)
		return
	}
	log.Infof("start server at %s successfully", *addrRPC)

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

func parseCfg() *dn.Cfg {
	if *addrRPC == "" {
		log.Fatalln("ConfigManager rpc addr must be set")
	}

	return &dn.Cfg{
		RpcAddr: *addrRPC,
		DataDir: path.Join(*storagePath, *addrRPC),
		CMAddr:  "http://" + *primaryCM,
	}
}
