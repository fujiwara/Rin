package rin

import (
	"log"
	"time"
)

type Option struct {
	Batch        bool
	MaxExecCount int
	MaxExecTime  time.Duration
}

func (o *Option) Mode() string {
	if o.Batch {
		return "Batch"
	} else {
		return "Worker"
	}
}

func (o *Option) NewBreakerFunc() func() bool {
	if !o.Batch {
		// worker mode. never break
		return func() bool { return true }
	}
	var (
		tm      *time.Timer
		counter int
	)
	if o.MaxExecTime != 0 {
		log.Printf("[debug] New timer waked after %s", o.MaxExecTime)
		tm = time.NewTimer(o.MaxExecTime)
	}
	return func() bool {
		counter++
		if o.MaxExecCount > 0 && counter > o.MaxExecCount {
			log.Printf("[debug] Execute %d times.", counter)
			log.Printf("[info] Execution count reached to max exection count %d", o.MaxExecCount)
			return true
		}
		if tm != nil {
			select {
			case <-tm.C:
				log.Printf("[info] Timeout reached to max execution time %s", o.MaxExecTime)
				return true
			default:
			}
		}
		return false
	}
}
