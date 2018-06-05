package master

import (
	"fmt"
	"github.com/gogo/protobuf/proto"
	"github.com/google/btree"
	"github.com/tiglabs/baudengine/proto/masterpb"
	"github.com/tiglabs/baudengine/proto/metapb"
	"github.com/tiglabs/baudengine/util"
	"github.com/tiglabs/baudengine/util/deepcopy"
	"github.com/tiglabs/baudengine/util/log"
	"sync"
	"time"
)

const (
	PREFIX_PARTITION   = "schema partition "
	defaultBTreeDegree = 64
	FIXED_REPLICA_NUM  = 1 // TODO: add config
)

type Partition struct {
	*metapb.Partition // !!! Do not directly operate the Replicas，must be firstly take the propertyLock

	Leader *metapb.Replica `json:"leader"`

	// TODO: temporary policy, finally using global task to replace it
	taskFlag    bool
	taskTimeout time.Time

	LastHeartbeat time.Time `json:"last_heartbeat"`
	propertyLock  sync.RWMutex
}

func NewPartition(dbId metapb.DBID, spaceId metapb.SpaceID, startSlot, endSlot metapb.SlotID) (*Partition, error) {
	partId, err := GetIdGeneratorSingle(nil).GenID()
	if err != nil {
		log.Error("generate partition id is failed. err:[%v]", err)
		return nil, ErrGenIdFailed
	}

	metaPartition := &metapb.Partition{
		ID:        metapb.PartitionID(partId),
		DB:        dbId,
		Space:     spaceId,
		StartSlot: metapb.SlotID(startSlot),
		EndSlot:   metapb.SlotID(endSlot),
		Replicas:  make([]metapb.Replica, 0),
		Status:    metapb.PA_READONLY,
	}
	return NewPartitionByMeta(metaPartition), nil
}

func NewPartitionByMeta(metaPartition *metapb.Partition) *Partition {
	return &Partition{
		Partition: metaPartition,
	}
}

func (p *Partition) batchPersistent(batch Batch) error {
	p.propertyLock.Lock()
	defer p.propertyLock.Unlock()

	key, val, err := doMetaMarshal(p.Partition)
	if err != nil {
		return err
	}
	batch.Put(key, val)

	return nil
}

func (p *Partition) erase(store Store) error {
	p.propertyLock.Lock()
	defer p.propertyLock.Unlock()

	key := []byte(fmt.Sprintf("%s%d", PREFIX_PARTITION, p.ID))
	if err := store.Delete(key); err != nil {
		log.Error("fail to delete partition[%v] from store. err:[%v]", p.Partition, err)
		return ErrLocalDbOpsFailed
	}

	return nil
}

func (p *Partition) deleteReplica(store Store, metaReplicas ...*metapb.Replica) error {
	p.propertyLock.Lock()
	defer p.propertyLock.Unlock()

	copy := deepcopy.Iface(p.Partition).(*metapb.Partition)
	for i := len(copy.Replicas) - 1; i >= 0; i-- {
		for _, metaReplica := range metaReplicas {
			if copy.Replicas[i].ID == metaReplica.ID {
				copy.Replicas = append(copy.Replicas[:i], copy.Replicas[i+1:]...)
			}
		}
	}

	key, val, err := doMetaMarshal(copy)
	if err != nil {
		return err
	}
	if err := store.Put(key, val); err != nil {
		return err
	}

	p.Partition = copy
	return nil
}

func (p *Partition) addReplica(store Store, metaReplicas ...*metapb.Replica) error {
	p.propertyLock.Lock()
	defer p.propertyLock.Unlock()

	copy := deepcopy.Iface(p.Partition).(*metapb.Partition)
	for _, r := range metaReplicas {
		copy.Replicas = append(copy.Replicas, *r)
	}

	key, val, err := doMetaMarshal(copy)
	if err != nil {
		return err
	}
	if err := store.Put(key, val); err != nil {
		return err
	}

	p.Partition = copy

	return nil
}

// updating policy :
// 1. update the leader and replicas group when confVer of partitionInfo is greater than confVer of cluster partition
//    or current cluster partition have no leader
// 2. update only the leader when confVer of partitionInfo is equals to confVer of cluster partition,
//    and the leader of partitionInfo is exists in replica group of current cluster partition
func (p *Partition) UpdateReplicaGroupByCond(store Store, info *masterpb.PartitionInfo,
	leaderReplica *metapb.Replica) (verExpired, updateOk bool) {
	p.propertyLock.Lock()
	defer p.propertyLock.Unlock()

	if info.Epoch.ConfVersion < p.Epoch.ConfVersion ||
		(info.Epoch.ConfVersion == p.Epoch.ConfVersion && p.Leader != nil) {
		return true, false
	}

	copy := deepcopy.Iface(p.Partition).(*metapb.Partition)
	copy.Epoch = info.Epoch
	copy.Status = info.Status

	copy.Replicas = make([]metapb.Replica, 0, len(info.RaftStatus.Followers)+1)
	copy.Replicas = append(copy.Replicas, info.RaftStatus.Replica)
	for _, follower := range info.RaftStatus.Followers {
		replica := &follower.Replica
		copy.Replicas = append(copy.Replicas, *replica)
	}
	key, val, err := doMetaMarshal(copy)
	if err != nil {
		return false, false
	}
	if err := store.Put(key, val); err != nil {
		return false, false
	}

	p.Partition = copy

	p.taskFlag = false
	p.taskTimeout = time.Time{}

	p.LastHeartbeat = time.Now()
	p.Leader = leaderReplica

	return false, true
}

func (p *Partition) ValidateAndUpdateLeaderByCond(info *masterpb.PartitionInfo,
	leaderReplica *metapb.Replica) (verExpired, illegal, updateOk bool) {
	p.propertyLock.Lock()
	defer p.propertyLock.Unlock()

	if info.Epoch.ConfVersion != p.Epoch.ConfVersion {
		return true, false, false
	}

	if p.Leader == nil {
		return false, false, false
	}

	var leaderExists bool
	for _, replica := range p.Replicas {
		if replica.ID == leaderReplica.ID {
			leaderExists = true
			break
		}
	}
	if !leaderExists {
		return false, true, false
	}

	p.LastHeartbeat = time.Now()
	p.Leader = leaderReplica

	return false, false, true
}

func (p *Partition) countReplicas() int {
	p.propertyLock.RLock()
	defer p.propertyLock.RUnlock()

	return len(p.Replicas)
}

func (p *Partition) getAllReplicas() []*metapb.Replica {
	p.propertyLock.RLock()
	defer p.propertyLock.RUnlock()

	replicas := make([]*metapb.Replica, 0, len(p.Replicas))
	for _, metaReplica := range p.Replicas {
		replicas = append(replicas, &metaReplica)
	}

	return replicas
}

func (p *Partition) pickLeaderNodeId() metapb.NodeID {
	p.propertyLock.RLock()
	defer p.propertyLock.RUnlock()

	if p.Leader != nil {
		return p.Leader.NodeID
	} else {
		return 0
	}
}

func (p *Partition) findReplicaById(replicaId metapb.ReplicaID) *metapb.Replica {
	p.propertyLock.RLock()
	defer p.propertyLock.RUnlock()

	for _, replica := range p.Replicas {
		if replica.ID == replicaId {
			return &replica
		}
	}

	return nil
}

func (p *Partition) takeChangeMemberTask() bool {
	p.propertyLock.Lock()
	defer p.propertyLock.Unlock()

	if p.taskFlag == false || time.Now().Sub(p.taskTimeout) >= 30*time.Second {
		p.taskFlag = true
		p.taskTimeout = time.Now()
		return true
	}

	return false
}

// internal use, need to write lock external
func doMetaMarshal(p *metapb.Partition) ([]byte, []byte, error) {
	val, err := proto.Marshal(p)
	if err != nil {
		log.Error("fail to marshal partition[%v]. err:[%v]", p, err)
		return nil, nil, err
	}
	key := []byte(fmt.Sprintf("%s%d", PREFIX_PARTITION, p.ID))

	return key, val, err
}

type PartitionCache struct {
	lock       sync.RWMutex
	partitions map[metapb.PartitionID]*Partition
}

func NewPartitionCache() *PartitionCache {
	return &PartitionCache{
		partitions: make(map[metapb.PartitionID]*Partition),
	}
}

func (c *PartitionCache) FindPartitionById(partitionId metapb.PartitionID) *Partition {
	c.lock.RLock()
	defer c.lock.RUnlock()

	p, ok := c.partitions[partitionId]
	if !ok {
		return nil
	}

	return p
}

func (c *PartitionCache) AddPartition(partition *Partition) {
	c.lock.Lock()
	defer c.lock.Unlock()

	c.partitions[partition.ID] = partition
}

func (c *PartitionCache) GetAllPartitions() *[]Partition {
	c.lock.RLock()
	defer c.lock.RUnlock()

	partitions := make([]Partition, 0, len(c.partitions))
	for _, partition := range c.partitions {
		log.Debug("api get all partition[%p] ", partition)
		partitions = append(partitions, *partition)
	}

	return &partitions
}

func (c *PartitionCache) GetAllMetaPartitions() *[]metapb.Partition {
	c.lock.RLock()
	defer c.lock.RUnlock()

	partitions := make([]metapb.Partition, 0, len(c.partitions))
	for _, partition := range c.partitions {
		partitions = append(partitions, *partition.Partition)
	}

	return &partitions
}

func (c *PartitionCache) Recovery(store Store) ([]*Partition, error) {
	prefix := []byte(PREFIX_PARTITION)
	startKey, limitKey := util.BytesPrefix(prefix)

	resultPartitions := make([]*Partition, 0)

	iterator := store.Scan(startKey, limitKey)
	defer iterator.Release()
	for iterator.Next() {
		if iterator.Key() == nil {
			log.Error("partition store key is nil. never happened!!!")
			continue
		}

		val := iterator.Value()
		metaPartition := new(metapb.Partition)
		if err := proto.Unmarshal(val, metaPartition); err != nil {
			log.Error("fail to unmarshal partition from store. err[%v]", err)
			return nil, ErrInternalError
		}

		resultPartitions = append(resultPartitions, NewPartitionByMeta(metaPartition))
	}

	return resultPartitions, nil
}

func (c *PartitionCache) Clear() {
	c.lock.Lock()
	defer c.lock.Unlock()

	c.partitions = make(map[metapb.PartitionID]*Partition)
}

type PartitionItem struct {
	partition *Partition
}

// Less returns true if the region start key is greater than the other.
// So we will sort the region with start key reversely.
func (r *PartitionItem) Less(other btree.Item) bool {
	left := r.partition.StartSlot
	right := other.(*PartitionItem).partition.StartSlot
	//return bytes.Compare(left, right) > 0
	return left > right
}

func (r *PartitionItem) Contains(slot metapb.SlotID) bool {
	start, end := r.partition.StartSlot, r.partition.EndSlot
	//return bytes.Compare(key, start) >= 0 && bytes.Compare(key, end) < 0
	return slot >= start && slot < end
}

type PartitionTree struct {
	tree *btree.BTree
}

func NewPartitionTree() *PartitionTree {
	return &PartitionTree{
		tree: btree.New(defaultBTreeDegree),
	}
}

func (t *PartitionTree) length() int {
	return t.tree.Len()
}

// update updates the tree with the region.
// It finds and deletes all the overlapped regions first, and then
// insert the region.
func (t *PartitionTree) update(rng *Partition) {
	item := &PartitionItem{partition: rng}

	result := t.find(rng)
	if result == nil {
		result = item
	}

	var overlaps []*PartitionItem
	var count int
	t.tree.DescendLessOrEqual(result, func(i btree.Item) bool {
		over := i.(*PartitionItem)
		//if bytes.Compare(rng.EndSlot, over.region.StartKey) <= 0 {
		if rng.EndSlot <= over.partition.StartSlot {
			return false
		}
		overlaps = append(overlaps, over)
		count++
		return true
	})

	if count > 2 {
		log.Warn("=========many overlaps ranges %v, new range[%v]", overlaps, rng)
	}
	for _, item := range overlaps {
		t.tree.Delete(item)
	}

	t.tree.ReplaceOrInsert(item)
}

// remove removes a region if the region is in the tree.
// It will do nothing if it cannot find the region or the found region
// is not the same with the region.
func (t *PartitionTree) remove(rng *Partition) {
	result := t.find(rng)
	if result == nil || result.partition.ID != rng.ID {
		return
	}

	t.tree.Delete(result)
}

// search returns a region that contains the key.
func (t *PartitionTree) search(slot metapb.SlotID) *Partition {
	rng := &Partition{
		Partition: &metapb.Partition{
			StartSlot: slot,
		},
	}
	log.Debug("################### len=%v", t.tree.Len())
	result := t.find(rng)
	if result == nil {
		return nil
	}
	return result.partition
}

func (t *PartitionTree) multipleSearch(slot metapb.SlotID, num int) []*Partition {
	rng := &Partition{
		Partition: &metapb.Partition{
			StartSlot: slot,
		},
	}
	results := t.ascendScan(rng, num)
	var ranges = make([]*Partition, 0, num)
	var endSlot metapb.SlotID
	var isFound = false
	for _, r := range results {
		//if len(endKey) != 0 {
		if isFound {
			//if bytes.Compare(r.region.GetStartKey(), endKey) != 0 {
			if r.partition.StartSlot != endSlot {
				break
			}
		}
		ranges = append(ranges, r.partition)
		endSlot = r.partition.EndSlot
		isFound = true
	}
	return ranges
}

// This is a helper function to find an item.
func (t *PartitionTree) find(rng *Partition) *PartitionItem {
	item := &PartitionItem{partition: rng}

	var result *PartitionItem
	t.tree.AscendGreaterOrEqual(item, func(i btree.Item) bool {
		result = i.(*PartitionItem)
		return false
	})

	log.Debug("####range find: result=%v, startkey=%v", result, rng.StartSlot)

	if result != nil {
		log.Debug("####range find: result range =%v, startkey=%v", result.partition, rng.StartSlot)
	}

	if result == nil || !result.Contains(rng.StartSlot) {
		return nil
	}

	return result
}

func (t *PartitionTree) ascendScan(rng *Partition, num int) []*PartitionItem {
	result := t.find(rng)
	if result == nil {
		return nil
	}

	var results []*PartitionItem
	//var firstItem *rangeItem
	results = make([]*PartitionItem, 0, num)
	count := 0
	t.tree.DescendLessOrEqual(result, func(i btree.Item) bool {
		results = append(results, i.(*PartitionItem))
		count++
		if count == num {
			return false
		} else {
			return true
		}
	})
	return results
}
