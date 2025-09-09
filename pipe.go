package pipe

import (
	"fmt"
	"sync"
)

const MaxItems = 1000

type Producer interface {
	Next() (items []any, cookie int, err error)
	Commit(cookie int) error
}

type Consumer interface {
	Process(items []any) error
}

type batch struct {
	items []any
	cookies []int
}

type commitTask struct {
	mu      sync.Mutex
	cond    *sync.Cond
	isReady bool
	err     error
	cookies []int
}

func (t *commitTask) wait() error{
	t.mu.Lock()
	defer t.mu.Unlock()
	for !t.isReady {
		t.cond.Wait()
	}

	return t.err
}

func (t * commitTask) signal() {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.isReady = true
	t.cond.Broadcast()
}

func newCommitTask(cookies []int) *commitTask {
	t := &commitTask{cookies: cookies}
	t.cond = sync.NewCond(&t.mu)
	return t
}

type processTask struct {
	CommitTask *commitTask
	items         []any
}

const sizeBatchQueue = 100

func Pipe(p Producer, c Consumer) error {
	var wg sync.WaitGroup
	var errRes error

	readyBatchCh := make(chan batch, sizeBatchQueue)
	commitTaskCh := make(chan *commitTask, sizeBatchQueue)
	errNextCh := make(chan error, 1)
	errCommitCh := make(chan error, 1)
	stopHandlerNextCh := make(chan bool, 1)

	defer func() {
		close(readyBatchCh)
		close(commitTaskCh)
		close(errNextCh)
		close(errCommitCh)
		close(stopHandlerNextCh)
	}()

	wg.Add(2)
	go handlerNext(p, &wg, readyBatchCh, stopHandlerNextCh, errNextCh)
	go handlerCommit(p, &wg, commitTaskCh, errCommitCh)

Loop:
	for {
		select {
		case err := <-errCommitCh:
			fmt.Println("pipe errCommitCh")
			errRes = err
			stopHandlerNextCh <- true
			break Loop

		case err := <-errNextCh:
			errRes = err
			break Loop

		case btch := <-readyBatchCh:
			cmt := newCommitTask(btch.cookies)
			prt := processTask{
				CommitTask: cmt,
				items:         btch.items,
			}
			commitTaskCh <- cmt
			wg.Add(1)
			go handlerProcess(c, &wg, prt)
		}
	}

	for i:=0; i< len(readyBatchCh); i++ {
		btch := <-readyBatchCh
		cmt := newCommitTask(btch.cookies)
		prt := processTask{
			CommitTask: cmt,
			items:         btch.items,
		}
		commitTaskCh <- cmt
		wg.Add(1)
		go handlerProcess(c, &wg, prt)
	}
	commitTaskCh <- nil

	wg.Wait()
	if err := <-errCommitCh; err!=nil {
		errRes = err
	}

	return errRes
}

func handlerNext(p Producer, wg *sync.WaitGroup, readyBatch chan batch, stopCh chan bool, errCh chan error) {
	defer wg.Done()
	var buf []any
	var cookies []int

Loop:
	for {
		select {
		case <-stopCh:
			return
		default:
			items, cookie, err := p.Next()
			if err != nil {
				errCh <- fmt.Errorf("read error: %w", err)
				return
			}

			if len(items) == 0 {
				break Loop
			}

			if len(buf)+len(items) > MaxItems {
				readyBatch <- batch{items: buf, cookies: cookies}
				buf = []any{}
				cookies = []int{}
			}

			buf = append(buf, items...)
			cookies = append(cookies, cookie)
		}
	}

	if len(buf) > 0 {
		readyBatch <- batch{items: buf, cookies: cookies}
	}
	errCh <- nil
}

func handlerProcess(c Consumer, wg *sync.WaitGroup, prt processTask) {
	defer wg.Done()
	defer prt.CommitTask.signal()

	if err := c.Process(prt.items); err != nil {
		prt.CommitTask.mu.Lock()
		defer prt.CommitTask.mu.Unlock()
		prt.CommitTask.err = fmt.Errorf("process error: %w", err)
		prt.CommitTask.isReady = true
		return
	}

	prt.CommitTask.mu.Lock()
	defer prt.CommitTask.mu.Unlock()
	prt.CommitTask.isReady = true
}


func handlerCommit(p Producer, wg *sync.WaitGroup, cmt chan *commitTask, errCh chan error) {
	defer wg.Done()

	for {
		task := <-cmt
		if task == nil {
			errCh <- nil
			return
		}
		
		if err := commit(p, task); err != nil {
			errCh <- err
			return
		}
	}
}

func commit(p Producer, task *commitTask) error {
	if err := task.wait(); err != nil {
		return err
	}

	for _, cookie := range task.cookies {
		if err := p.Commit(cookie); err != nil {
			return fmt.Errorf("commit error for cookie %d: %w", cookie, err)
		}
	}

	return nil
}
