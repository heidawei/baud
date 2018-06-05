package master

import (
	"context"
	"github.com/tiglabs/baudengine/proto/metapb"
	"github.com/tiglabs/baudengine/util/deepcopy"
	"github.com/tiglabs/baudengine/util/log"
	"runtime/debug"
	"sync"
	"sync/atomic"
)

const (
	PARTITION_CHANNEL_LIMIT = 1000
)

const (
	EVENT_TYPE_INVALID = iota
	EVENT_TYPE_PARTITION_CREATE
	EVENT_TYPE_PARTITION_DELETE       // partition is in cluster
	EVENT_TYPE_FORCE_PARTITION_DELETE // partition is not in cluster
)

var (
	processorManagerSingle     *ProcessorManager
	processorManagerSingleLock sync.Mutex
	processorManagerSingleDone uint32
)

type ProcessorManager struct {
	ctx    context.Context
	cancel context.CancelFunc

	pp *PartitionProcessor

	isStarted bool
	wg        sync.WaitGroup
}

func GetPMSingle(cluster *Cluster) *ProcessorManager {
	if processorManagerSingle != nil {
		return processorManagerSingle
	}
	if atomic.LoadUint32(&processorManagerSingleDone) == 1 {
		return processorManagerSingle
	}

	processorManagerSingleLock.Lock()
	defer processorManagerSingleLock.Unlock()

	if atomic.LoadUint32(&processorManagerSingleDone) == 0 {
		if cluster == nil {
			log.Error("cluster should not be nil at first time when create ProcessorManager single")
		}

		pm := new(ProcessorManager)
		pm.ctx, pm.cancel = context.WithCancel(context.Background())
		pm.pp = NewPartitionProcessor(pm.ctx, pm.cancel, cluster)

		processorManagerSingle = pm
		pm.start()

		atomic.StoreUint32(&processorManagerSingleDone, 1)

		log.Info("ProcessorManager single has started")
	}

	return processorManagerSingle
}

func (pm *ProcessorManager) Close() {
	if !pm.isStarted {
		return
	}
	pm.isStarted = false

	pm.cancel()
	pm.wg.Wait()

	pm.pp.Close()

	processorManagerSingleLock.Lock()
	defer processorManagerSingleLock.Unlock()

	processorManagerSingle = nil
	atomic.StoreUint32(&processorManagerSingleDone, 0)

	log.Info("ProcessorManager single has closed")
}

func (pm *ProcessorManager) start() {
	pm.wg.Add(1)
	go func() {
		defer pm.wg.Done()
		defer func() {
			if e := recover(); e != nil {
				log.Error("recover partition processor panic. e:[%s]\n stack:[%s]", e, debug.Stack())
			}
		}()

		pm.pp.Run()
	}()

	pm.isStarted = true

	log.Info("Processor manager has started")
}

func (pm *ProcessorManager) PushEvent(event *ProcessorEvent) error {
	if event == nil {
		log.Error("empty event")
		return ErrInternalError
	}

	if !pm.isStarted {
		log.Error("processor manager is not started")
		return ErrInternalError
	}

	if event.typ == EVENT_TYPE_PARTITION_CREATE ||
		event.typ == EVENT_TYPE_PARTITION_DELETE ||
		event.typ == EVENT_TYPE_FORCE_PARTITION_DELETE {

		if len(pm.pp.eventCh) >= PARTITION_CHANNEL_LIMIT*0.9 {
			log.Error("partition channel will full, reject event[%v]", event)
			return ErrSysBusy
		}

		pm.pp.eventCh <- event

	} else {
		log.Error("processor received invalid event type[%v]", event.typ)
		return ErrInternalError
	}

	return nil
}

type ProcessorEvent struct {
	typ  int
	body interface{}
}

func NewPartitionCreateEvent(partition *Partition) *ProcessorEvent {
	return &ProcessorEvent{
		typ:  EVENT_TYPE_PARTITION_CREATE,
		body: partition,
	}
}

// internal use
type PartitionDeleteBody struct {
	partitionId    metapb.PartitionID
	leaderNodeId   metapb.NodeID
	replicaRpcAddr string
	replica        *metapb.Replica
}

func NewPartitionDeleteEvent(partitionId metapb.PartitionID, leaderNodeId metapb.NodeID,
	replica *metapb.Replica) *ProcessorEvent {
	return &ProcessorEvent{
		typ: EVENT_TYPE_PARTITION_DELETE,
		body: &PartitionDeleteBody{
			partitionId:  partitionId,
			leaderNodeId: leaderNodeId,
			replica:      replica,
		},
	}
}

func NewForcePartitionDeleteEvent(partitionId metapb.PartitionID, replicaRpcAddr string,
	replica *metapb.Replica) *ProcessorEvent {
	return &ProcessorEvent{
		typ: EVENT_TYPE_FORCE_PARTITION_DELETE,
		body: &PartitionDeleteBody{
			partitionId:    partitionId,
			replicaRpcAddr: replicaRpcAddr,
			replica:        replica,
		},
	}
}

type Processor interface {
	Run()
	Close()
}

type PartitionProcessor struct {
	ctx        context.Context
	cancelFunc context.CancelFunc
	wg         sync.WaitGroup

	eventCh        chan *ProcessorEvent
	cluster        *Cluster
	serverSelector Selector
	jdos           DCOS
}

func NewPartitionProcessor(ctx context.Context, cancel context.CancelFunc, cluster *Cluster) *PartitionProcessor {

	p := &PartitionProcessor{
		ctx:            ctx,
		cancelFunc:     cancel,
		eventCh:        make(chan *ProcessorEvent, PARTITION_CHANNEL_LIMIT),
		cluster:        cluster,
		serverSelector: NewIdleSelector(),
		jdos:           new(JDOS),
	}

	return p
}

func (p *PartitionProcessor) Run() {
	log.Info("Partition Processor is running")

	for {
		select {
		case <-p.ctx.Done():
			log.Info("Partition Processor exit")
			return
		case event, opened := <-p.eventCh:
			if !opened {
				log.Debug("closed partition processor event channel")
				return
			}

			if event.typ == EVENT_TYPE_PARTITION_CREATE {

				p.wg.Add(1)
				go func() {
					defer p.wg.Done()

					partitionToCreate := event.body.(*Partition)
					psToCreate := p.serverSelector.SelectTarget(p.cluster.PsCache.GetAllServers(), partitionToCreate.ID)
					if psToCreate == nil {
						log.Error("Can not distribute suitable ps node")
						// TODO: calling jdos api to allocate a container asynchronously
						return
					}
					log.Debug("psToCreate node[%v], all ps:[%v]", psToCreate.ID, p.cluster.PsCache.GetAllServers())

					p.createPartition(partitionToCreate, psToCreate)
				}()

			} else if event.typ == EVENT_TYPE_PARTITION_DELETE {

				p.wg.Add(1)
				go func() {
					defer p.wg.Done()

					body := event.body.(*PartitionDeleteBody)
					p.deletePartition(body.partitionId, body.leaderNodeId, body.replica)
				}()

			} else if event.typ == EVENT_TYPE_FORCE_PARTITION_DELETE {

				p.wg.Add(1)
				go func() {
					defer p.wg.Done()

					body := event.body.(*PartitionDeleteBody)
					p.forceDeletePartition(body.partitionId, body.replicaRpcAddr, body.replica)
				}()
			}
		}
	}
}

func (p *PartitionProcessor) Close() {
	if p.eventCh != nil {
		close(p.eventCh)
	}

	p.wg.Wait()
}

func (p *PartitionProcessor) createPartition(partitionToCreate *Partition, psToCreate *PartitionServer) {
	leaderPS := p.cluster.PsCache.FindServerById(partitionToCreate.pickLeaderNodeId())
	// leaderPS is nil when create first partition

	replicaId, err := GetIdGeneratorSingle(nil).GenID()
	if err != nil {
		log.Error("fail to generate new replica ßid. err:[%v]", err)
		return
	}
	var newMetaReplica = &metapb.Replica{ID: metapb.ReplicaID(replicaId), NodeID: psToCreate.ID,
		ReplicaAddrs: metapb.ReplicaAddrs{
			HeartbeatAddr: psToCreate.HeartbeatAddr,
			ReplicateAddr: psToCreate.ReplicateAddr,
			RpcAddr:       psToCreate.RpcAddr,
			AdminAddr:     psToCreate.AdminAddr,
		}}

	partitionCopy := deepcopy.Iface(partitionToCreate.Partition).(*metapb.Partition)
	partitionCopy.Replicas = append(partitionCopy.Replicas, *newMetaReplica)
	if err := GetPSRpcClientSingle(nil).CreatePartition(psToCreate.getRpcAddr(),
		partitionCopy); err != nil {
		log.Error("Rpc fail to create partition[%v] into ps. err:[%v]",
			partitionToCreate.Partition, err)
		return
	}

	if leaderPS != nil {
		if err := GetPSRpcClientSingle(nil).AddReplica(leaderPS.getRpcAddr(), partitionToCreate.ID,
			&psToCreate.ReplicaAddrs, newMetaReplica.ID, newMetaReplica.NodeID); err != nil {
			log.Error("Rpc fail to add replica[%v] into leader ps. err[%v]", newMetaReplica, err)
			return
		}
	}
}

func (p *PartitionProcessor) deletePartition(partitionId metapb.PartitionID, leaderNodeId metapb.NodeID,
	replica *metapb.Replica) {
	leaderPS := p.cluster.PsCache.FindServerById(leaderNodeId)
	if leaderPS == nil {
		log.Debug("can not find leader ps when notify deleting replicas to leader")
		return
	}
	psToDelete := p.cluster.PsCache.FindServerById(replica.NodeID)
	if psToDelete == nil {
		log.Debug("can not find replica[%v] ps needed to deleted", replica.NodeID)
		return
	}

	if err := GetPSRpcClientSingle(nil).RemoveReplica(leaderPS.getRpcAddr(), partitionId,
		&psToDelete.ReplicaAddrs, replica.ID, replica.NodeID); err != nil {
		log.Error("Rpc fail to remove replica[%v] from ps. err[%v]", replica.ID, err)
		return
	}

	if err := GetPSRpcClientSingle(nil).DeletePartition(psToDelete.getRpcAddr(),
		partitionId); err != nil {
		log.Error("Rpc fail to delete partition[%v] from ps. err:[%v]", partitionId, err)
		return
	}
}

func (p *PartitionProcessor) forceDeletePartition(partitionId metapb.PartitionID, replicaRpcAddr string,
	replica *metapb.Replica) {
	if err := GetPSRpcClientSingle(nil).DeletePartition(replicaRpcAddr, partitionId); err != nil {
		log.Error("Rpc fail to delete partition[%v] from ps. err:[%v]", partitionId, err)
		return
	}
}
