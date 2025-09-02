package pipe

import (
	"errors"
	"testing"
)

// MockProducer - упрощенная реализация Producer
type MockProducer struct {
	batches   [][]any
	cookies   []int
	nextIndex int
	commitLog []int
	returnErr error
	commitErr error
}

func NewMockProducer(batches [][]any) *MockProducer {
	cookies := make([]int, len(batches))
	for i := range cookies {
		cookies[i] = i + 1
	}
	return &MockProducer{
		batches:   batches,
		cookies:   cookies,
		commitLog: []int{},
	}
}

func (m *MockProducer) Next() ([]any, int, error) {
	if m.returnErr != nil {
		return nil, 0, m.returnErr
	}

	if m.nextIndex >= len(m.batches) {
		return []any{}, 0, nil
	}

	batch := m.batches[m.nextIndex]
	cookie := m.cookies[m.nextIndex]
	m.nextIndex++
	return batch, cookie, nil
}

func (m *MockProducer) Commit(cookie int) error {
	if m.commitErr != nil {
		return m.commitErr
	}
	m.commitLog = append(m.commitLog, cookie)
	return nil
}

type MockConsumer struct {
	processedBatches [][]any
	returnErr        error
}

func NewMockConsumer() *MockConsumer {
	return &MockConsumer{
		processedBatches: [][]any{},
	}
}

func (m *MockConsumer) Process(items []any) error {
	if m.returnErr != nil {
		return m.returnErr
	}

	m.processedBatches = append(m.processedBatches, items)
	return nil
}

func TestPipe_BasicFunctionality(t *testing.T) {
	batches := [][]any{
		{"item1", "item2"},
		{"item3", "item4", "item5"},
		{"item6"},
		{"item7", "item8", "item9", "item10"},
	}

	producer := NewMockProducer(batches)
	consumer := NewMockConsumer()

	err := Pipe(producer, consumer)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	totalItems := 0
	for _, batch := range batches {
		totalItems += len(batch)
	}

	processedItems := 0
	for _, batch := range consumer.processedBatches {
		processedItems += len(batch)
	}

	if processedItems != totalItems {
		t.Errorf("Expected %d items processed, got %d", totalItems, processedItems)
	}

	if len(producer.commitLog) != len(batches) {
		t.Errorf("Expected %d commits, got %d", len(batches), len(producer.commitLog))
	}

	for i, cookie := range producer.commitLog {
		expectedCookie := i + 1
		if cookie != expectedCookie {
			t.Errorf("Commit %d: expected cookie %d, got %d", i, expectedCookie, cookie)
		}
	}
}

func TestPipe_ProducerError(t *testing.T) {
	producer := NewMockProducer([][]any{{"item1", "item2"}})
	producer.returnErr = errors.New("producer error")

	consumer := NewMockConsumer()

	err := Pipe(producer, consumer)
	if err == nil {
		t.Fatal("Expected error from producer, got nil")
	}

	if err.Error() != "read error: producer error" {
		t.Errorf("Unexpected error message: %v", err)
	}
}

func TestPipe_ConsumerError(t *testing.T) {
	batches := [][]any{
		{"item1", "item2"},
		{"item3", "item4"},
	}

	producer := NewMockProducer(batches)
	consumer := NewMockConsumer()
	consumer.returnErr = errors.New("consumer error")

	err := Pipe(producer, consumer)
	if err == nil {
		t.Fatal("Expected error from consumer, got nil")
	}

	expectedError := "process error: consumer error"
	if err.Error() != expectedError {
		t.Errorf("Expected error '%s', got '%v'", expectedError, err)
	}

	if len(producer.commitLog) > 0 {
		t.Errorf("No commits should be made on consumer error, got %d commits", len(producer.commitLog))
	}
}

func TestPipe_CommitError(t *testing.T) {
	producer := NewMockProducer([][]any{{"item1", "item2"}})
	producer.commitErr = errors.New("commit error")

	consumer := NewMockConsumer()

	err := Pipe(producer, consumer)
	if err == nil {
		t.Fatal("Expected commit error, got nil")
	}

	expectedError := "commit error for cookie 1: commit error"
	if err.Error() != expectedError {
		t.Errorf("Expected error '%s', got '%v'", expectedError, err)
	}
}

func TestPipe_EmptyBatches(t *testing.T) {
	producer := NewMockProducer([][]any{})
	consumer := NewMockConsumer()

	err := Pipe(producer, consumer)
	if err != nil {
		t.Fatalf("Unexpected error with empty producer: %v", err)
	}

	if len(consumer.processedBatches) > 0 {
		t.Errorf("No batches should be processed for empty producer, got %d", len(consumer.processedBatches))
	}
}

func TestPipe_CommitOrder(t *testing.T) {
	batches := [][]any{
		make([]any, 300),
		make([]any, 400),
		make([]any, 1000),
		make([]any, 200),
	}

	producer := NewMockProducer(batches)
	consumer := NewMockConsumer()

	err := Pipe(producer, consumer)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	for i, cookie := range producer.commitLog {
		expectedCookie := i + 1
		if cookie != expectedCookie {
			t.Errorf("Commit order violation at position %d: expected %d, got %d",
				i, expectedCookie, cookie)
		}
	}
}
