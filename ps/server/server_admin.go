package server

import (
	"context"
	"fmt"

	"github.com/gogo/protobuf/proto"

	"github.com/tiglabs/baudengine/proto/metapb"
	"github.com/tiglabs/baudengine/proto/pspb"
	"github.com/tiglabs/baudengine/util/log"
	raftproto "github.com/tiglabs/raft/proto"
)

// CreatePartition admin grpc service for create partition
func (s *Server) CreatePartition(ctx context.Context, request *pspb.CreatePartitionRequest) (*pspb.CreatePartitionResponse, error) {
	log.Debug("CreatePartition recive request: %s", request)

	response := &pspb.CreatePartitionResponse{
		ResponseHeader: metapb.ResponseHeader{
			ReqId: request.ReqId,
			Code:  metapb.RESP_CODE_OK,
		},
	}

	if s.stopping.Get() {
		response.Code = metapb.RESP_CODE_SERVER_STOP
		response.Message = "server is stopping"
	} else {
		s.adminEventCh <- request
	}

	return response, nil
}

// DeletePartition admin grpc service for delete partition
func (s *Server) DeletePartition(ctx context.Context, request *pspb.DeletePartitionRequest) (*pspb.DeletePartitionResponse, error) {
	log.Debug("DeletePartition recive request: %s", request)

	response := &pspb.DeletePartitionResponse{
		ResponseHeader: metapb.ResponseHeader{
			ReqId: request.ReqId,
			Code:  metapb.RESP_CODE_OK,
		},
	}

	if s.stopping.Get() {
		response.Code = metapb.RESP_CODE_SERVER_STOP
		response.Message = "server is stopping"
	} else {
		s.adminEventCh <- request
	}

	return response, nil
}

// ChangeReplica admin grpc service for change replica of partition
func (s *Server) ChangeReplica(ctx context.Context, request *pspb.ChangeReplicaRequest) (*pspb.ChangeReplicaResponse, error) {
	log.Debug("ChangeReplica recive request: %s", request)

	response := &pspb.ChangeReplicaResponse{
		ResponseHeader: metapb.ResponseHeader{
			ReqId: request.ReqId,
			Code:  metapb.RESP_CODE_OK,
		},
	}

	if s.stopping.Get() {
		response.Code = metapb.RESP_CODE_SERVER_STOP
		response.Message = "server is stopping"
		return response, nil
	}
	if _, ok := s.partitions.Load(request.PartitionID); !ok {
		response.Code = metapb.PS_RESP_CODE_NO_PARTITION
		response.Message = fmt.Sprintf("node[%d] has not found partition[%d]", s.NodeID, request.PartitionID)
		return response, nil
	}
	if !s.RaftServer.IsLeader(request.PartitionID) {
		response.Code = metapb.PS_RESP_CODE_NOT_LEADER
		response.Message = fmt.Sprintf("node[%d] is not leader of partition[%d]", s.NodeID, request.PartitionID)
		return response, nil
	}

	var ccType raftproto.ConfChangeType
	switch request.Type {
	case pspb.ReplicaChangeType_Add:
		ccType = raftproto.ConfAddNode
	case pspb.ReplicaChangeType_Remove:
		ccType = raftproto.ConfRemoveNode
	}
	peer := raftproto.Peer{Type: raftproto.PeerNormal, ID: uint64(request.Replica.NodeID), PeerID: request.Replica.ID}
	ctxData, _ := request.Replica.Marshal()
	s.RaftServer.ChangeMember(request.PartitionID, ccType, peer, ctxData)

	return response, nil
}

// ChangeLeader admin grpc service for change leader of partition
func (s *Server) ChangeLeader(ctx context.Context, request *pspb.ChangeLeaderRequest) (*pspb.ChangeLeaderResponse, error) {
	response := &pspb.ChangeLeaderResponse{
		ResponseHeader: metapb.ResponseHeader{
			ReqId: request.ReqId,
			Code:  metapb.RESP_CODE_OK,
		},
	}

	if s.stopping.Get() {
		response.Code = metapb.RESP_CODE_SERVER_STOP
		response.Message = "server is stopping"
		return response, nil
	}
	if _, ok := s.partitions.Load(request.PartitionID); !ok {
		response.Code = metapb.PS_RESP_CODE_NO_PARTITION
		response.Message = fmt.Sprintf("node[%d] has not found partition[%d]", s.NodeID, request.PartitionID)
		return response, nil
	}

	s.RaftServer.TryToLeader(request.PartitionID)
	return response, nil
}

func (s *Server) doPartitionCreate(p metapb.Partition) {
	partition, _ := BuildPartitionStore(s.PartitionStore, s, p)
	if _, ok := s.partitions.LoadOrStore(p.ID, partition); ok {
		partition.Close()
	} else {
		for _, r := range p.Replicas {
			s.RaftResolver.AddNode(r.NodeID, r.ReplicaAddrs)
		}

		partition.Start()
	}
}

func (s *Server) doPartitionDelete(id metapb.PartitionID) {
	if p, ok := s.partitions.Load(id); ok {
		s.partitions.Delete(id)
		p.(PartitionStore).Close()

		for _, r := range p.(PartitionStore).GetMeta().Replicas {
			s.RaftResolver.DeleteNode(r.NodeID)
		}
	}

	s.meta.clear(id)
}

func (s *Server) destroyExcludePartition(partitions []metapb.Partition) {
	ids := s.meta.getAllPartitions()

	for _, id := range ids {
		delete := true
		for _, p := range partitions {
			if p.ID == id {
				delete = false
				break
			}
		}

		if delete {
			s.meta.clear(id)
		}
	}
}

func (s *Server) reset() {
	s.meta.clearAll()
}

func (s *Server) adminEventHandler() {
	for {
		select {
		case <-s.Ctx.Done():
			return

		case event := <-s.adminEventCh:
			s.doAdminEvent(event)
		}
	}
}

func (s *Server) doAdminEvent(event proto.Message) {
	if s.stopping.Get() {
		return
	}

	switch e := event.(type) {
	case *pspb.CreatePartitionRequest:
		if p, ok := s.partitions.Load(e.Partition.ID); ok {
			if p.(PartitionStore).GetMeta().Status != metapb.PA_INVALID {
				return
			}

			p.(PartitionStore).Close()
			s.partitions.Delete(e.Partition.ID)
		}

		s.doPartitionCreate(e.Partition)
		s.MasterHeartbeat.Trigger()

	case *pspb.DeletePartitionRequest:
		s.doPartitionDelete(e.ID)
		s.MasterHeartbeat.Trigger()
	}
}
