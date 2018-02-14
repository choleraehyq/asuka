package cm

import (
	"context"
	"net/http"
	"time"

	log "github.com/sirupsen/logrus"

	"sync/atomic"

	"github.com/choleraehyq/asuka/pb/metapb"
)

func (s *Server) startRPC() {
	defer func() {
		if err := recover(); err != nil {
			log.Errorf("rpc: crash, errors:\n %+v", err)
		}
	}()

	twirpHandler := metapb.NewMetaServiceServer(MetaRPCServer{}, nil)
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

type MetaRPCServer struct {
}

func (s *MetaRPCServer) GetLatestTerm(context.Context, *GetLatestTermReq) (*GetLatestTermResp, error) {

}

func (s *MetaRPCServer) Join(context.Context, *JoinReq) (*JoinResp, error) {

}

func (s *MetaRPCServer) NeedSplit(context.Context, *NeedSplitReq) (*NeedSplitResp, error) {

}

func (s *MetaRPCServer) DoSplit(context.Context, *DoSplitReq) (*DoSplitResp, error) {

}

func (s *MetaRPCServer) ConfChange(context.Context, *ConfChangeReq) (*ConfChangeResp, error) {

}

func (s *MetaRPCServer) QueryLocation(context.Context, *QueryLocationReq) (*QueryLocationResp, error) {

}
