package rin_test

import (
	"testing"
	"time"

	rin "github.com/fujiwara/Rin"
)

func TestBreakerForWorker(t *testing.T) {
	opt := &rin.Option{
		Batch:        false,
		MaxExecTime:  time.Second,
		MaxExecCount: 3,
	}
	breaker := opt.NewBreakerFunc()
	for i := 0; i < 10000; i++ {
		if breaker() {
			t.Error("must be true in worker mode")
		}
	}
	time.Sleep(2 * time.Second)
	if breaker() {
		t.Error("must be true in worker mode")
	}
}

func TestBreakerForBatchMaxImports(t *testing.T) {
	opt := &rin.Option{
		Batch:        true,
		MaxExecCount: 3,
	}
	breaker := opt.NewBreakerFunc()
	for i := 0; i < 3; i++ {
		if breaker() {
			t.Error("must be false in batch mode")
		}
	}
	if breaker() == false {
		t.Error("must breaks after 3 times called")
	}
}

func TestBreakerForBatchMaxExec(t *testing.T) {
	opt := &rin.Option{
		Batch:       true,
		MaxExecTime: time.Second,
	}
	breaker := opt.NewBreakerFunc()
	for i := 0; i < 10; i++ {
		if breaker() {
			t.Error("must be false in batch mode")
		}
	}
	time.Sleep(2 * time.Second)
	if breaker() == false {
		t.Error("must breaks after 1 second")
	}
}
