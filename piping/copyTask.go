package piping

import (
	"l2met/store"
)

//Low level task for copying buckets from the input channel in a sender
//to all of it's output channels
type CopyTask struct {
	sender  Sender
	control chan bool
}

//Takes a sender to operate on
func NewCopyTask(sender Sender) (cp *CopyTask) {
	cp = &CopyTask{
		sender:  sender,
		control: make(chan bool)}
	return cp
}

func (cp *CopyTask) copy(b *store.Bucket) {
	for _, channel := range cp.sender.GetOutputChannels() {
		channel <- b
	}
}

//Begins copying buckets from input channel to all output channels
func (cp *CopyTask) Start() {
	for {
		select {
		case <-cp.control:
			return
		case b := <-cp.sender.GetSenderChannel():
			cp.copy(b)
		}
	}
}

func (cp *CopyTask) Stop() {
	cp.control <- true
}
