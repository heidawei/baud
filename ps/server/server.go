package server

import (
	"context"
	"fmt"
	"net"
	"sort"
	"sync"
	"time"

	"github.com/gogo/protobuf/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"

	"github.com/tiglabs/baudengine/proto/masterpb"
	"github.com/tiglabs/baudengine/proto/metapb"
	"github.com/tiglabs/baudengine/proto/pspb"
	"github.com/tiglabs/baudengine/ps/metric"
	"github.com/tiglabs/baudengine/util"
	"github.com/tiglabs/baudengine/util/atomic"
	"github.com/tiglabs/baudengine/util/build"
	"github.com/tiglabs/baudengine/util/log"
	"github.com/tiglabs/baudengine/util/netutil"
	"github.com/tiglabs/baudengine/util/routine"
	"github.com/tiglabs/baudengine/util/rpc"
	"github.com/tiglabs/baudengine/util/timeutil"
	"github.com/tiglabs/baudengine/util/uuid"
	"github.com/tiglabs/raft"
)

const (
	registerTimeout = 10 * time.Second
)

// Server partition server
type Server struct {
	Config
	ctx       context.Context
	ctxCancel context.CancelFunc

	ip           string
	masterLeader string
	meta         *serverMeta

	raftResolver *RaftResolver
	raftConfig   *raft.Config
	raftServer   *raft.RaftServer

	connMgr         *rpc.ConnectionMgr
	adminServer     *grpc.Server
	masterClient    *rpc.Client
	masterHeartbeat *heartbeatWork

	systemMetric *metric.SystemMetric
	partitions   sync.Map
	adminEventCh chan proto.Message

	stopping atomic.AtomicBool
}

// NewServer create server instance
func NewServer(conf *Config) *Server {
	s := &Server{
		Config:       *conf,
		ip:           netutil.GetPrivateIP().String(),
		meta:         newServerMeta(conf.StorePath),
		raftResolver: NewRaftResolver(),
		systemMetric: metric.NewSystemMetric(conf.StorePath, conf.DiskQuota),
		adminEventCh: make(chan proto.Message, 64),
	}
	s.ctx, s.ctxCancel = context.WithCancel(context.Background())

	serverOpt := rpc.DefaultServerOption
	serverOpt.ClusterID = conf.ClusterID
	s.adminServer = rpc.NewGrpcServer(&serverOpt)

	connMgrOpt := rpc.DefaultManagerOption
	s.connMgr = rpc.NewConnectionMgr(s.ctx, &connMgrOpt)
	clientOpt := rpc.DefaultClientOption
	clientOpt.Compression = true
	clientOpt.ClusterID = conf.ClusterID
	clientOpt.ConnectMgr = s.connMgr
	clientOpt.CreateFunc = func(cc *grpc.ClientConn) interface{} { return masterpb.NewMasterRpcClient(cc) }
	s.masterClient = rpc.NewClient(1, &clientOpt)
	s.masterHeartbeat = newHeartbeatWork(s)

	return s
}

// Start start server
func (s *Server) Start() error {
	return s.doStart(true)
}

func (s *Server) doStart(init bool) error {
	// load meta data
	if init {
		metaInfo := s.meta.getInfo()
		log.Info("Server load meta from file is: %s", metaInfo)
		if metaInfo.NodeID > 0 {
			s.NodeID = metaInfo.NodeID
		}
		if metaInfo.ClusterID != s.ClusterID {
			s.reset()
		}
	}

	var initPartitions []metapb.Partition
	// do register to master
	if s.MasterServer != "" {
		registerResp, err := s.register()
		if err != nil {
			return err
		}
		s.meta.reset(&pspb.MetaInfo{ClusterID: s.ClusterID, NodeID: registerResp.NodeID})

		log.Info("Server get register response from master is: %s", registerResp)

		initPartitions = registerResp.Partitions
		if s.NodeID != registerResp.NodeID {
			s.NodeID = registerResp.NodeID
			if s.raftServer != nil {
				s.raftServer.Stop()
				s.raftServer = nil
			}
		}
	}

	// create raft server
	if s.isRaftStore && s.raftServer == nil {
		rc := raft.DefaultConfig()
		rc.NodeID = uint64(s.NodeID)
		rc.LeaseCheck = true
		rc.HeartbeatAddr = fmt.Sprintf(":%d", s.RaftHeartbeatPort)
		rc.ReplicateAddr = fmt.Sprintf(":%d", s.RaftReplicatePort)
		rc.Resolver = s.raftResolver
		if s.RaftReplicaConcurrency > 0 {
			rc.MaxReplConcurrency = s.RaftReplicaConcurrency
		}
		if s.RaftSnapConcurrency > 0 {
			rc.MaxSnapConcurrency = s.RaftSnapConcurrency
		}
		if s.RaftHeartbeatInterval > 0 {
			rc.TickInterval = time.Millisecond * time.Duration(s.RaftHeartbeatInterval)
		}
		if s.RaftRetainLogs > 0 {
			rc.RetainLogs = s.RaftRetainLogs
		}

		rs, err := raft.NewRaftServer(rc)
		if err != nil {
			return fmt.Errorf("boot raft server failed, error: %s", err)
		}
		s.raftServer = rs
		s.raftConfig = rc
	}

	// clear old partition
	if len(initPartitions) == 0 {
		s.reset()
	} else {
		s.destroyExcludePartition(initPartitions)
	}
	// create and recover partitions
	s.recoverPartitions(initPartitions)

	// Start server
	if init {
		if ln, err := net.Listen("tcp", fmt.Sprintf(":%d", s.AdminPort)); err != nil {
			return fmt.Errorf("Server failed to listen admin port: %s", err)
		} else {
			pspb.RegisterAdminGrpcServer(s.adminServer, s)
			reflection.Register(s.adminServer)
			go func() {
				if err = s.adminServer.Serve(ln); err != nil {
					log.Fatal("Server failed to start admin grpc: %s", err)
				}
			}()
			log.Info("Server admin grpc listen on: %s", fmt.Sprintf(":%d", s.AdminPort))
		}

		routine.RunWorkDaemon("ADMIN-EVENTHANDLER", s.adminEventHandler, s.ctx.Done())
	}

	// start heartbeat to master
	s.stopping.Set(false)
	if s.MasterServer != "" {
		s.masterHeartbeat.start()
		s.masterHeartbeat.trigger()
	}

	log.Info("Baud server successful startup...")
	return nil
}

// Stop stop server
func (s *Server) Close() error {
	s.stopping.Set(true)
	s.ctxCancel()

	if s.masterHeartbeat != nil {
		s.masterHeartbeat.stop()
	}
	if s.adminServer != nil {
		s.adminServer.GracefulStop()
	}

	routine.Stop()
	s.closeAllRange()

	if s.raftServer != nil {
		s.raftServer.Stop()
	}
	if s.masterClient != nil {
		s.masterClient.Close()
	}
	if s.connMgr != nil {
		s.connMgr.Close()
	}

	return nil
}

func (s *Server) closeAllRange() {
	s.partitions.Range(func(key, value interface{}) bool {
		p := value.(PartitionStore)
		meta := p.GetMeta()
		p.Close()
		s.partitions.Delete(meta.ID)

		for _, r := range meta.Replicas {
			s.raftResolver.DeleteNode(r.NodeID)
		}
		return true
	})
}

func (s *Server) register() (*masterpb.PSRegisterResponse, error) {
	retryOpt := util.DefaultRetryOption
	retryOpt.MaxRetries = 10
	retryOpt.Context = s.ctx

	buildInfo := build.GetInfo()
	request := &masterpb.PSRegisterRequest{
		RequestHeader: metapb.RequestHeader{ReqId: uuid.FlakeUUID()},
		NodeID:        s.NodeID,
		Ip:            s.ip,
		RuntimeInfo: masterpb.RuntimeInfo{
			AppVersion: buildInfo.AppVersion,
			GoVersion:  buildInfo.GoVersion,
			Platform:   buildInfo.Platform,
			StartTime:  timeutil.FormatNow(),
		},
	}
	var response *masterpb.PSRegisterResponse

	err := util.RetryMaxAttempt(&retryOpt, func() error {
		masterAddr := s.MasterServer
		if s.masterLeader != "" {
			masterAddr = s.masterLeader
		}
		masterClient, err := s.masterClient.GetGrpcClient(masterAddr)
		if err != nil {
			return fmt.Errorf("get master register rpc client[%s] error: %s", masterAddr, err)
		}

		goCtx, cancel := context.WithTimeout(s.ctx, registerTimeout)
		resp, err := masterClient.(masterpb.MasterRpcClient).PSRegister(goCtx, request)
		cancel()

		if err != nil {
			return fmt.Errorf("master register requeset[%s] failed error: %s", request.ReqId, err)
		}
		if resp.Code != metapb.RESP_CODE_OK {
			if resp.Error.NoLeader != nil {
				s.masterLeader = ""
			} else if resp.Error.NotLeader != nil {
				s.masterLeader = resp.Error.NotLeader.LeaderAddr
			}
			return fmt.Errorf("master register requeset[%s] ack code not ok, response is: %s", request.ReqId, resp)
		}

		response = resp
		return nil
	})

	if err != nil {
		log.Error(err.Error())
	}
	return response, err
}

func (s *Server) recoverPartitions(partitions []metapb.Partition) {
	// sort by partition id
	sort.Sort(partitionByIDSlice(partitions))
	wg := new(sync.WaitGroup)
	wg.Add(len(partitions))

	// parallel execution recovery
	for i := 0; i < len(partitions); i++ {
		p := partitions[i]
		routine.RunWorkAsync("RECOVER-PARTITION", func() {
			defer wg.Done()

			log.Debug("starting recover partition[%d]...", p.ID)
			s.doPartitionCreate(p)
		}, routine.LogPanic(false))
	}

	wg.Wait()
}

func (s *Server) restart() {
	// do close
	s.stopping.Set(true)
	s.masterHeartbeat.stop()
	s.masterClient.Close()

	// clear admin event channel
	endFlag := false
	for {
		select {
		case <-s.adminEventCh:
		default:
			endFlag = true
		}

		if endFlag {
			break
		}
	}

	// do start
	s.closeAllRange()
	if err := s.doStart(false); err != nil {
		panic(fmt.Errorf("restart error: %s", err))
	}
}
