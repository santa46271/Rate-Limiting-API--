package pipe

import "fmt"

const MaxItems = 1000

type Producer interface {
	Next() (items []any, cookie int, err error)
	Commit(cookie int) error
}

type Consumer interface {
	Process(items []any) error
}

func Pipe(p Producer, c Consumer) error {
	var buf []any
	var cookies []int

	for {
		items, cookie, err := p.Next()
		if err != nil {
			return fmt.Errorf("read error: %w", err)
		}

		if len(items) == 0 {
			break
		}

		if len(buf)+len(items) > MaxItems {
			if err := processAndCommitBatch(c, p, buf, cookies); err != nil {
				return err
			}
			buf = buf[:0]
			cookies = cookies[:0]
		}

		buf = append(buf, items...)
		cookies = append(cookies, cookie)
	}

	if len(buf) > 0 {
		if err := processAndCommitBatch(c, p, buf, cookies); err != nil {
			return err
		}
	}

	return nil
}

func processAndCommitBatch(c Consumer, p Producer, buf []any, cookies []int) error {
	if err := c.Process(buf); err != nil {
		return fmt.Errorf("process error: %w", err)
	}

	for _, cookie := range cookies {
		if err := p.Commit(cookie); err != nil {
			return fmt.Errorf("commit error for cookie %d: %w", cookie, err)
		}
	}

	return nil
}
