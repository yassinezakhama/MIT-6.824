package main

import (
	"math/rand"
	"time"
)

func main() {
	rand.Seed(time.Now().UnixNano())

	count := 0
	finished := 0
	ch := make(chan bool)
	for i := 0; i < 10; i++ {
		go func() {
			ch <- requestVote()
		}()
	}

	// this solution is not good because as soon as count reaches 5, this thread will stop listening on the channel
	// and the remaining threads will stay blocked. If this program was a long running service, this would leak threads
	for count < 5 && finished < 10 {
		v := <-ch
		// locking is not necessary because memory is not shared
		if v {
			count += 1
		}
		finished += 1
	}

	if count >= 5 {
		println("received 5+ votes!")
	} else {
		println("lost")
	}
}

func requestVote() bool {
	time.Sleep(time.Duration(rand.Intn(100)) * time.Millisecond)
	return rand.Int()%2 == 0
}
