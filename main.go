package main

import (
	"fmt"
	"runtime"
	"time"

	"github.com/google/uuid"
)

type WorkRequest struct {
	Id string
}

type Scheduler struct {
    Work chan WorkRequest
	WorkerQueue chan chan WorkRequest
    QuitChan chan bool	
}

func (s *Scheduler) Start() {
	go func() {
        for {
			s.WorkerQueue <- s.Work

			select {
			case work := <- s.Work:
			    fmt.Println("goroutine_count", runtime.NumGoroutine())
				fmt.Printf("work %s start\n", work.Id)
				time.Sleep(1 * time.Second)
				fmt.Printf("work %s finished\n", work.Id)
			case <-s.QuitChan:
				return
			}
		}		
	} ()
}

func NewScheduler(workerQueue chan chan WorkRequest) *Scheduler {
	return &Scheduler{
		Work: make(chan WorkRequest),
		WorkerQueue: workerQueue,
		QuitChan:  make(chan bool),
	}
}

func Dispatch() {
    workerQueue := make(chan chan WorkRequest, 500)	
	scheduler := NewScheduler(workerQueue)
	scheduler.Start()

	go func() {
		for {
			work := WorkRequest{
				Id: uuid.New().String(),
			}
			go func() {
				worker := <- workerQueue
				worker <- work
			} ()
			time.Sleep(10 * time.Millisecond)
		}
	} ()
}

func main() {
	Dispatch()
    select {}	
}
