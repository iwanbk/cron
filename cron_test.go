package cron

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"
)

// Many tests schedule a job for every second, and then wait at most a second
// for it to run.  This amount is just slightly larger than 1 second to
// compensate for a few milliseconds of runtime.
const ONE_SECOND = 1*time.Second + 10*time.Millisecond

// Start and stop cron with no entries.
func TestNoEntries(t *testing.T) {
	cron := New()
	cron.Start()

	select {
	case <-time.After(ONE_SECOND):
		t.FailNow()
	case <-stop(cron):
	}
}

// Start, stop, then add an entry. Verify entry doesn't run.
func TestStopCausesJobsToNotRun(t *testing.T) {
	wg := &sync.WaitGroup{}
	wg.Add(1)

	cron := New()
	cron.Start()
	cron.Stop()
	cron.AddFunc("* * * * * ?", func(ctx context.Context) { wg.Done() })

	select {
	case <-time.After(ONE_SECOND):
		// No job ran!
	case <-wait(wg):
		t.FailNow()
	}
}

// Add a job, start cron, expect it runs.
func TestAddBeforeRunning(t *testing.T) {
	wg := &sync.WaitGroup{}
	wg.Add(1)

	cron := New()
	cron.AddFunc("* * * * * ?", func(ctx context.Context) { wg.Done() })
	cron.Start()
	defer cron.Stop()

	// Give cron 2 seconds to run our job (which is always activated).
	select {
	case <-time.After(ONE_SECOND):
		t.FailNow()
	case <-wait(wg):
	}
}

// Start cron, add a job, expect it runs.
func TestAddWhileRunning(t *testing.T) {
	wg := &sync.WaitGroup{}
	wg.Add(1)

	cron := New()
	cron.Start()
	defer cron.Stop()
	cron.AddFunc("* * * * * ?", func(ctx context.Context) { wg.Done() })

	select {
	case <-time.After(ONE_SECOND):
		t.FailNow()
	case <-wait(wg):
	}
}

// Add a job, remove a job, start cron, expect nothing runs.
func TestRemoveBeforeRunning(t *testing.T) {
	wg := &sync.WaitGroup{}
	wg.Add(1)

	cron := New()
	id, _ := cron.AddFunc("* * * * * ?", func(ctx context.Context) { wg.Done() })
	cron.Remove(id)
	cron.Start()
	defer cron.Stop()

	select {
	case <-time.After(ONE_SECOND):
		// Success, shouldn't run
	case <-wait(wg):
		t.FailNow()
	}
}

// Start cron, add a job, remove it, expect it doesn't run.
func TestRemoveWhileRunning(t *testing.T) {
	wg := &sync.WaitGroup{}
	wg.Add(1)

	cron := New()
	cron.Start()
	defer cron.Stop()
	id, _ := cron.AddFunc("* * * * * ?", func(ctx context.Context) { wg.Done() })
	cron.Remove(id)

	select {
	case <-time.After(ONE_SECOND):
	case <-wait(wg):
		t.FailNow()
	}
}

// Test timing with Entries.
func TestSnapshotEntries(t *testing.T) {
	wg := &sync.WaitGroup{}
	wg.Add(1)

	cron := New()
	cron.AddFunc("@every 2s", func(ctx context.Context) { wg.Done() })
	cron.Start()
	defer cron.Stop()

	// Cron should fire in 2 seconds. After 1 second, call Entries.
	select {
	case <-time.After(ONE_SECOND):
		cron.Entries()
	}

	// Even though Entries was called, the cron should fire at the 2 second mark.
	select {
	case <-time.After(ONE_SECOND):
		t.FailNow()
	case <-wait(wg):
	}

}

// Test that the entries are correctly sorted.
// Add a bunch of long-in-the-future entries, and an immediate entry, and ensure
// that the immediate entry runs immediately.
// Also: Test that multiple jobs run in the same instant.
func TestMultipleEntries(t *testing.T) {
	wg := &sync.WaitGroup{}
	wg.Add(2)

	cron := New()
	cron.AddFunc("0 0 0 1 1 ?", func(ctx context.Context) {})
	cron.AddFunc("* * * * * ?", func(ctx context.Context) { wg.Done() })
	id1, _ := cron.AddFunc("* * * * * ?", func(ctx context.Context) { t.Fatal() })
	id2, _ := cron.AddFunc("* * * * * ?", func(ctx context.Context) { t.Fatal() })
	cron.AddFunc("0 0 0 31 12 ?", func(ctx context.Context) {})
	cron.AddFunc("* * * * * ?", func(ctx context.Context) { wg.Done() })

	cron.Remove(id1)
	cron.Start()
	cron.Remove(id2)
	defer cron.Stop()

	select {
	case <-time.After(ONE_SECOND):
		t.FailNow()
	case <-wait(wg):
	}
}

// Test running the same job twice.
func TestRunningJobTwice(t *testing.T) {
	wg := &sync.WaitGroup{}
	wg.Add(2)

	cron := New()
	cron.AddFunc("0 0 0 1 1 ?", func(ctx context.Context) {})
	cron.AddFunc("0 0 0 31 12 ?", func(ctx context.Context) {})
	cron.AddFunc("* * * * * ?", func(ctx context.Context) { wg.Done() })

	cron.Start()
	defer cron.Stop()

	select {
	case <-time.After(2 * ONE_SECOND):
		t.FailNow()
	case <-wait(wg):
	}
}

func TestRunningMultipleSchedules(t *testing.T) {
	wg := &sync.WaitGroup{}
	wg.Add(2)

	cron := New()
	cron.AddFunc("0 0 0 1 1 ?", func(ctx context.Context) {})
	cron.AddFunc("0 0 0 31 12 ?", func(ctx context.Context) {})
	cron.AddFunc("* * * * * ?", func(ctx context.Context) { wg.Done() })
	cron.Schedule(Every(time.Minute), FuncJob(func(ctx context.Context) {}))
	cron.Schedule(Every(time.Second), FuncJob(func(ctx context.Context) { wg.Done() }))
	cron.Schedule(Every(time.Hour), FuncJob(func(ctx context.Context) {}))

	cron.Start()
	defer cron.Stop()

	select {
	case <-time.After(2 * ONE_SECOND):
		t.FailNow()
	case <-wait(wg):
	}
}

type once struct {
	count int
}

func (o *once) Next(t time.Time) time.Time {
	o.count++
	if o.count <= 1 {
		return t.Add(1 * time.Second)
	}
	return t.Add(100 * time.Hour)
}

// Test that we can wait job to be finished
func TestStopWait(t *testing.T) {
	const (
		jobDuration = 3 * time.Second
	)

	tester := func(cron *Cron, waiter func() error) error {
		var (
			finished  = false               // just to be verbose that initial value is `false`
			startedCh = make(chan struct{}) // channel to indicate has been started
		)
		cron.Schedule(&once{}, FuncJob(func(ctx context.Context) {
			startedCh <- struct{}{}
			time.Sleep(jobDuration)
			finished = true
		}))

		cron.Start()
		<-startedCh // wait to be started, before stopping it

		err := waiter()
		if err != nil {
			return fmt.Errorf("WaitStop failed: %v", err)
		}
		if !finished {
			return fmt.Errorf("the job is not finished yet")
		}
		return nil
	}

	// make sure  `Stop` can't wait properly
	cron := New()
	err := tester(cron, func() error {
		cron.Stop()
		return nil
	})

	if err == nil {
		t.Errorf("Stop expect error, but got nil")
	}

	// make sure it `WaitStop` can wait properly
	cron = New()
	err = tester(cron, func() error {
		return cron.StopWait(2 * jobDuration)
	})
	if err != nil {
		t.Errorf("WaitStop expect nil but got error: %v", err)
	}
}

// Test that the cron is run in the local time zone (as opposed to UTC).
func TestLocalTimezone(t *testing.T) {
	wg := &sync.WaitGroup{}
	wg.Add(1)

	now := time.Now().Local()
	spec := fmt.Sprintf("%d %d %d %d %d ?",
		now.Second()+1, now.Minute(), now.Hour(), now.Day(), now.Month())

	cron := New()
	cron.AddFunc(spec, func(ctx context.Context) { wg.Done() })
	cron.Start()
	defer cron.Stop()

	select {
	case <-time.After(ONE_SECOND):
		t.FailNow()
	case <-wait(wg):
	}
}

type testJob struct {
	wg   *sync.WaitGroup
	name string
}

func (t testJob) Run(ctx context.Context) {
	t.wg.Done()
}

// Simple test using Runnables.
func TestJob(t *testing.T) {
	wg := &sync.WaitGroup{}
	wg.Add(1)

	cron := New()
	cron.AddJob("0 0 0 30 Feb ?", testJob{wg, "job0"})
	cron.AddJob("0 0 0 1 1 ?", testJob{wg, "job1"})
	cron.AddJob("* * * * * ?", testJob{wg, "job2"})
	cron.AddJob("1 0 0 1 1 ?", testJob{wg, "job3"})
	cron.Schedule(Every(5*time.Second+5*time.Nanosecond), testJob{wg, "job4"})
	cron.Schedule(Every(5*time.Minute), testJob{wg, "job5"})

	cron.Start()
	defer cron.Stop()

	select {
	case <-time.After(ONE_SECOND):
		t.FailNow()
	case <-wait(wg):
	}

	// Ensure the entries are in the right order.
	expecteds := []string{"job2", "job4", "job5", "job1", "job3", "job0"}

	var actuals []string
	for _, entry := range cron.Entries() {
		actuals = append(actuals, entry.Job.(testJob).name)
	}

	for i, expected := range expecteds {
		if actuals[i] != expected {
			t.Errorf("Jobs not in the right order.  (expected) %s != %s (actual)", expecteds, actuals)
			t.FailNow()
		}
	}
}

func wait(wg *sync.WaitGroup) chan bool {
	ch := make(chan bool)
	go func() {
		wg.Wait()
		ch <- true
	}()
	return ch
}

func stop(cron *Cron) chan bool {
	ch := make(chan bool)
	go func() {
		cron.Stop()
		ch <- true
	}()
	return ch
}
