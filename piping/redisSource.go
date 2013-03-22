package piping

import (
	"l2met/store"
	"l2met/utils"
	"time"
)

const keySep = "→"

//The RedisSource aggrogates and removes buckets from the first avalible 
//partition of a givin mailbox.
type RedisSource struct {
	sender        *SingleSender
	control       chan bool
	mailbox       string
	fetchInterval uint64
	Eager         bool
	partitioner   *RedisPartitioner
}

//Return a new redis source taking the fetchInterval (the time that the redis
//source waits to pull redis), numPartitions (the number of partitions that
//the redis mailbox is devided into), lockTTL (the time that a lock on a
//partition lives (genrally less then 5 seconds), and mailbox (the name of
//The redis mailbox that it is using
func NewRedisSource(fetchInterval uint64, numPartitions uint64, lockTTL uint64, mailbox string) (r *RedisSource) {
	r = &RedisSource{
		sender:        NewSingleSender(),
		control:       make(chan bool),
		Eager:         false,
		fetchInterval: fetchInterval,
		mailbox:       mailbox,
		partitioner:   NewRedisPartitioner(numPartitions, lockTTL, mailbox)}
	return r
}

//Starts the sender (the thing that gets the buckets aggrogated out of 
//the RedisSource) and the loop to pull items out of redis.
func (s *RedisSource) Start() {
	go s.runLoop()
	go s.sender.Start()
}

//Stops the senders work, any items that have been pulled into memory but not
//taken out of the output channels will remain in the out put channels
//Any items left in the mail box will be left alone.
func (s *RedisSource) Stop() {
	s.control <- true
	s.sender.Stop()
}

func (s *RedisSource) runLoop() {
	for {
		select {
		case <-s.control:
			utils.MeasureI("redis.source.stop", 1)
			return
		case <-time.Tick(time.Second * time.Duration(s.fetchInterval)):
			utils.MeasureI("redis.source.fetch.tick", 1)
			s.getMail(s.mailbox)
		}
	}
}

//Gets the output channel for the RedisSouce. This implementation only has
//a single output channel.
func (s *RedisSource) GetOutput() chan *store.Bucket {
	return s.sender.GetOutput()
}

func (s *RedisSource) getMail(mailbox string) {
	sc := s.sender.GetSenderChannel()
	defer utils.MeasureT("redis.source.getMailbox.time", time.Now())
	buckets, _ := store.EmptyMailboxPartition(mailbox, int(s.partitioner.LockPartition()))
	for _, b := range buckets {
		if s.Eager {
			b.Get()
		}
		sc <- b
	}
	utils.MeasureI("redis.source.outputChan.len", int64(len(s.sender.GetOutput())))
}
