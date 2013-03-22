package piping

import (
	"l2met/store"
)

//All async parts of the application implement this
//While it may be seen as overly abstract we are all terrible at concurrnecy
//and it should be kept in nice neat boxes.
type Task interface {
	Start()
	Stop()
}

//A partitioner is used to help with synchronisation between multiple parts
//of the application, currently it is used between the redis source and outlet
type Partitioner interface {
	//Get a Lock on a partition 
	LockPartition() uint64
	GetMailbox() string
	GetNumPartitions() uint64
}

//A Sender is responsible for the aggregation of Buckets to
//all of the output channels that it has a handel to.
//The SenderChannel is the interface into the Sender
type Sender interface {
	NewOutputChannel(name string, size uint64) chan *store.Bucket
	DeleteOutputChannel(name string)
	GetOutputChannels() map[string]chan *store.Bucket
	//Channel that feeds into the sender
	GetSenderChannel() chan *store.Bucket
}

//A Receiver is responsible for holding on to the incomming buckets in a givin
//componenet until it can get to it.
type Receiver interface {
	SetInput(input chan *store.Bucket)
	//Channel that feeds out of the reciver into your application logic
	GetReciverChannel() chan *store.Bucket
}

//A component that both recives and sends.
//An example would be a demultiplexer
//(takes a stream of objects and devides them up for work)
type Aggregator interface {
	Task
	Receiver
	Sender
}

//An outlet to something outside of the application
//PostgresOutlet is an example (which puts buckets into postgres)
//RedisOutlet is an example (which puts buckets into redis for distrubution)
type Outlet interface {
	Task
	Receiver
}

//A source creates buckets and and puts them into it's sender channel(s)
//Examples would include RedisSource and later HTTPSource and PostgresSource
type Source interface {
	Task
	Sender
}
