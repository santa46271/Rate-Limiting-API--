// Rate-Limiting и Буферизация API-вызовов
//
// Сервис генерирует поток событий, которые необходимо отправлять во внешний HTTP API. Этот API имеет строгое ограничение на количество запросов в секунду (Rate Limit). Ваша задача — реализовать механизм отправки, который не нарушает это ограничение и эффективно работает с потоком событий.
//
// // Event представляет собой единицу данных для отправки
// type Event struct {
//     ID string
//     // ... другие поля, не важные для логики задачи
// }
//
// type EventGenerator interface {
//     // Next возвращает следующее событие. 
//     // Если события закончились, возвращает io.EOF.
//     Next() (Event, error)
// }
//
// type APIClient interface {
//     // SendBatch отправляет батч событий.
//     // Внутри себя реализует логику HTTP-вызовов.
//     // Может вернуть ошибку, в том числе и "429 Too Many Requests".
//     SendBatch(batch []Event) error
// }
//
// Задача:
// Реализуйте функцию RateLimitedSender.
//
// func RateLimitedSender(g EventGenerator, client APIClient, maxBatchesPerSecond int) error {
//     // Ваш код здесь
// }
//
// Требования:
//
// Функция читает события из EventGenerator до тех пор, пока не будет возвращён io.EOF.
// События накапливаются в батчи (размер батча определите сами).
// Отправка батчей во внешний API должна происходить не чаще, чем maxBatchesPerSecond раз в секунду.
// При получении от API ошибки типа "429 Too Many Requests" необходимо реализовать механизм повторной отправки с экспоненциальным backoff.
// Функция должна вернуть ошибку в случае любой неустранимой проблемы (например, все попытки отправки провалились, или генератор вернул ошибку, отличную от io.EOF).
//
// Уточняющие вопросы:
// Вопрос 1: Какой размер батча следует использовать?
//
// Ответ: Размер батча не регламентирован. Нужно предложить разумное значение (например, 100, 1000) и обосновать его. Можно использовать фиксированный размер.
//
// Вопрос 2: Что именно возвращает SendBatch при ошибке "429"? Как отличить эту ошибку от других?
//
// Ответ: Для упрощения задачи, можно считать, что любая ошибка, возвращаемая SendBatch, — это ошибка, при которой стоит применить backoff (как "429").
//
// Вопрос 3: Нужно ли учитывать, что время обработки SendBatch может быть большим, и это может повлиять на RPS лимит?
//
// Ответ: Нет. Лимит maxBatchesPerSecond считается на основе интервалов между началом отправок батчей. Если отправка длится 200ms, а интервал 100ms, то лимит в 10 RPS всё равно будет соблюдён.
//
// Базовое решение:
package main

import (
    "context"
    "errors"
    "fmt"
    "io"
    "time"
)

const batchSize = 100 

func RateLimitedSender(g EventGenerator, client APIClient, maxBatchesPerSecond int) error {
    if maxBatchesPerSecond <= 0 {
        return errors.New("maxBatchesPerSecond должен быть больше 0")
    }

    minInterval := time.Second / time.Duration(maxBatchesPerSecond)
    ticker := time.NewTicker(minInterval)
    defer ticker.Stop() 

    var batch []Event

    for {
        event, err := g.Next()
        if err != nil {
            if errors.Is(err, io.EOF) {
                break             }
            return fmt.Errorf("получить событие не удаось: %w", err)
        }

        batch = append(batch, event)

        if len(batch) >= batchSize {
            <-ticker.C 
            err := sendWithBackoff(client, batch)
            if err != nil {
                return fmt.Errorf("все попытки отправки завершиись неудачей: %w", err)
            }
            batch = nil 
        }
    }

    if len(batch) > 0 {
        <-ticker.C
        err := sendWithBackoff(client, batch)
        if err != nil {
            return fmt.Errorf("отправка последнего батча завершиись неудачей: %w", err)
        }
    }

    return nil
}

func sendWithBackoff(client APIClient, batch []Event) error {
    baseDelay := time.Millisecond * 100
    maxAttempts := 5

    for attempt := 1; attempt <= maxAttempts; attempt++ {
        err := client.SendBatch(batch)
        if err == nil {
            return nil 
        }
        fmt.Printf("попытка %d отправки батча завершиись неудачей: %v. повторная попытка...\n", attempt, err)

        if attempt == maxAttempts {
            break 
        }

        backoffDelay := baseDelay * (1 << (attempt - 1)) 
        time.Sleep(backoffDelay)
    }
    return errors.New("количество попыток исчерпано")
}

// Примеры тестов: 

func TestRateLimitedSender_Success(t *testing.T) {
    gen := &eventGen{count: 250} // 250 событий -> EOF
    client := &testClient{}      // Всегда возвращает nil (успех)
    
    err := RateLimitedSender(gen, client, 10)
    
    if err != nil {
        t.Fatalf("Ожидался успех, получена ошибка: %v", err)
    }
    if client.calls != 3 { // 100 + 100 + 50
        t.Fatalf("Ожидалось 3 вызова SendBatch, получено: %d", client.calls)
    }
}

func TestRateLimitedSender_GeneratorError(t *testing.T) {
    gen := &errorGen{err: errors.New("network fail")} // Всегда ошибка
    client := &testClient{}
    
    err := RateLimitedSender(gen, client, 10)
    
    if err == nil || err.Error() != "network fail" {
        t.Fatalf("Ожидалась ошибка 'network fail', получено: %v", err)
    }
    if client.calls != 0 {
        t.Fatalf("SendBatch не должен вызываться, вызван %d раз", client.calls)
    }
}

func TestRateLimitedSender_SendErrorWithRetry(t *testing.T) {
    gen := &eventGen{count: 100} // 100 событий -> EOF
    client := &flakyClient{}     // Первый вызов - ошибка, второй - успех
    
    err := RateLimitedSender(gen, client, 10)
    
    if err != nil {
        t.Fatalf("Ожидался успех после ретрая, получена ошибка: %v", err)
    }
    if client.calls != 2 { // 1 ошибка + 1 успех
        t.Fatalf("Ожидалось 2 вызова SendBatch, получено: %d", client.calls)
    }
}

func TestRateLimitedSender_SendFatalError(t *testing.T) {
    gen := &eventGen{count: 100} // 100 событий -> EOF
    client := &errorClient{}     // Всегда ошибка
    
    err := RateLimitedSender(gen, client, 10)
    
    if err == nil {
        t.Fatal("Ожидалась ошибка после всех ретраев")
    }
    if client.calls != 5 { // 1 базовый + 4 ретрая
        t.Fatalf("Ожидалось 5 вызовов SendBatch, получено: %d", client.calls)
    }
}

