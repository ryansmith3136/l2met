package piping

import (
	"l2met/store"
)

type RedisOutlet struct {
	reciver     *SingleReciver
	control     chan bool
	partition   uint64
	mailbox     string
	partitioner *RedisPartitioner
}

//Redis outlet takes buckets from the input channel and addes them to the 
//specified mailbox on the next avalible partition 
func NewRedisOutlet(input chan *store.Bucket, numPartitions uint64, lockTTL uint64, mailbox string) (r *RedisOutlet) {
	r = &RedisOutlet{
		reciver:     NewSingleReciver(input),
		control:     make(chan bool),
		partitioner: NewRedisPartitioner(numPartitions, lockTTL, mailbox),
		mailbox:     mailbox}
	return r
}

func (r *RedisOutlet) Start() {
	go r.runPutBuckets()
	go r.reciver.Start()
}

func (r *RedisOutlet) Stop() {
	r.control <- true
	r.reciver.Stop()
}

func (r *RedisOutlet) runPutBuckets() {
	for {
		select {
		case <-r.control:
			return
		case b := <-r.reciver.input:
			store.PutBucket(b, r.mailbox, r.partitioner.GetNumPartitions())
		}
	}
}
