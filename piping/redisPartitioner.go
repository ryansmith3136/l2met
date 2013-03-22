package piping

import (
	"errors"
	"fmt"
	"l2met/store"
	"l2met/utils"
	"math/rand"
	"os"
	"time"
)

//Partition
type RedisPartitioner struct {
	mailbox          string
	numPartitions    uint64
	lockTTL          uint64
	currentPartition uint64
}

func NewRedisPartitioner(numPartitions uint64, lockTTL uint64, mailbox string) (p *RedisPartitioner) {
	p = &RedisPartitioner{
		mailbox:       mailbox,
		numPartitions: numPartitions,
		lockTTL:       lockTTL}
	return p
}

func (s *RedisPartitioner) GetCurrentPartition() uint64 {
	return s.currentPartition
}

func (s *RedisPartitioner) GetNumPartitions() uint64 {
	return s.numPartitions
}

func (s *RedisPartitioner) GetLockTTL() uint64 {
	return s.lockTTL
}

func (s *RedisPartitioner) GetLockString(mailbox string, partition uint64) string {
	lockString := fmt.Sprintf("lock.%s.%d", mailbox, partition)
	return lockString
}

func (s *RedisPartitioner) LockPartition() uint64 {
	partition, err := s.lockPartition()
	if err != nil {
		fmt.Printf("Unable to lock partition.")
		os.Exit(1)
	}
	s.currentPartition = partition
	return partition
}

func (s *RedisPartitioner) lockPartition() (uint64, error) {

	offset := uint64(rand.Int63())
	for {
		//use mod so that we can generate a random number and start at that
		//mailbox, other wise low numbered mailboxes get preference and if there
		//are less outlets the sources they will never get to the higher number
		//mailboxes
		partition := uint64(offset % s.numPartitions)
		locked, err := store.TryLock(s.mailbox, partition, s.lockTTL)
		utils.MeasureI("redis.partitioner.locked.id", int64(partition))
		if err != nil {
			return 0, err
		}
		if locked {
			return partition, nil
		}
		offset++
		time.Sleep(time.Second * 5)
	}
	return 0, errors.New("LockPartition impossible broke the loop.")
}
