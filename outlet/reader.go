package outlet

import "l2met/bucket"

type Reader interface {
	Start(chan<- *bucket.Bucket)
}
