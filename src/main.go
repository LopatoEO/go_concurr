// 1 Базовый пул воркеров
// Генератор (producer) кладёт `N` заданий (`Job{ID: 1..N}`) в канал `jobs`.
// `M` воркеров читают из `jobs`, обрабатывают через `callExternalService`, пишут `Result` в канал `results`.
// `main` собирает все результаты из `results` и печатает их.
// Завершиться корректно: дождаться всех воркеров и не уронить программу.

// 2 Потокобезопасная статистика + детектор гонок
// Добавить сборщик `Stats`: счётчики `panics` / `failed` и `map[workerID]int` (сколько заданий успешно сделал каждый воркер).
// Воркеры обновляют статистику конкурентно.

// 3 Ограничение параллелизма семафором
// Внешний сервис держит максимум `K` одновременных вызовов (например, `K=2` при `M=4` воркерах). Реализовать ограничение **семафором на буферизованном канале**.

// 4 Отмена через context + отсутствие утечек
// Протащить `context.Context` от `main` во все воркеры и в `callExternalService`.
// Завести общий таймаут операции через `context.WithTimeout` (например, 2–3 с).
// Воркеры обязаны **уважать** `ctx.Done()`: при отмене не начинать новую работу и не залипать.

// 5 Graceful shutdown
// Сервис должен корректно останавливаться по сигналу ОС (`SIGINT`/`SIGTERM`): перестать брать новые задания, дать текущим завершиться, но не висеть вечно.

package main

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"
)

type Job struct {
	ID int
}

type Stats struct {
	Panics  int
	Failed  int
	Success map[int]int
	lock    sync.Mutex
}

func (s *Stats) IncrementPanic() {
	s.lock.Lock()
	defer s.lock.Unlock()
	s.Panics++
}

func (s *Stats) IncrementFailed() {
	s.lock.Lock()
	defer s.lock.Unlock()
	s.Failed++
}

func (s *Stats) IncrementSuccess(workerID int) {
	s.lock.Lock()
	defer s.lock.Unlock()
	s.Success[workerID]++
}

type Result struct {
	JobID  int
	Result int
	Err    error
}

type WorkerPool struct {
	NumWorkers int
	Semaphore  chan struct{}
	Stats      *Stats
}

func NewWorkerPool(numWorkers, maxConcurrent int) *WorkerPool {
	return &WorkerPool{
		NumWorkers: numWorkers,
		Semaphore:  make(chan struct{}, maxConcurrent),
		Stats: &Stats{
			Success: make(map[int]int),
		},
	}
}

func (wp *WorkerPool) Start(intakeCtx context.Context, jobs <-chan Job, results chan<- Result) {
	var wg sync.WaitGroup
	done := make(chan struct{})

	workCtx, cancelWork := context.WithCancel(context.Background())

	for i := 1; i <= wp.NumWorkers; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			worker(intakeCtx, workCtx, workerID, jobs, results, wp.Stats, wp.Semaphore)
		}(i)
	}

	go func() {
		wg.Wait()
		close(results)
		close(done)
	}()

	go func() {
		<-intakeCtx.Done()

		timer := time.NewTimer(5 * time.Second)
		defer timer.Stop()
		select {
		case <-timer.C:
			cancelWork()
		case <-done:
			return
		}
	}()

}

func producer(ctx context.Context, jobs chan<- Job, N int) {
	defer close(jobs)

	for i := 1; i <= N; i++ {
		select {
		case <-ctx.Done():
			return
		case jobs <- Job{ID: i}:
		}
	}
}

func worker(
	intakeCtx context.Context,
	workCtx context.Context,
	id int,
	jobs <-chan Job,
	results chan<- Result,
	stats *Stats,
	sem chan struct{},
) {
	for {
		select {
		case <-intakeCtx.Done():
			return
		case j, ok := <-jobs:
			if !ok {
				return
			}

			if intakeCtx.Err() != nil {
				return
			}

			res, err, panicked := processJob(workCtx, j, sem)

			if panicked {
				stats.IncrementPanic()
			} else if err != nil {
				stats.IncrementFailed()
			} else {
				stats.IncrementSuccess(id)
			}

			results <- Result{
				JobID:  j.ID,
				Result: res,
				Err:    err,
			}
		}

	}
}

func processJob(
	ctx context.Context,
	j Job,
	sem chan struct{},
) (res int, err error, panicked bool) {

	select {
	case <-ctx.Done():
		return 0, ctx.Err(), false

	case sem <- struct{}{}:
	}

	defer func() {
		<-sem
	}()

	defer func() {
		if r := recover(); r != nil {
			panicked = true
			err = fmt.Errorf("job %d panic: %v", j.ID, r)
			res = 0
		}
	}()

	res, err = callExternalService(ctx, j)

	return res, err, false
}

func callExternalService(ctx context.Context, j Job) (int, error) {
	work := time.Duration(50+rand.Intn(250)) * time.Millisecond

	select {
	case <-ctx.Done():
		return 0, ctx.Err()
	case <-time.After(work):
	}

	roll := rand.Intn(10)
	switch {
	case roll <= 0:
		panic(fmt.Sprintf("сервис паникнул на job %d", j.ID))
	case roll <= 2:
		return 0, fmt.Errorf("сервис вернул ошибку на job %d", j.ID)
	default:
		return j.ID * j.ID, nil
	}
}

func main() {
	rand.Seed(time.Now().UnixNano())

	const (
		N = 1000 // количество заданий
		M = 4    // количество воркеров
		K = 2    // максимальное количество одновременных вызовов внешнего сервиса
	)

	signalCtx, stop := signal.NotifyContext(
		context.Background(),
		os.Interrupt,
		syscall.SIGTERM,
	)
	defer stop()

	intakeCtx, intakecancel := context.WithTimeout(signalCtx, 3*time.Second)
	defer intakecancel()

	jobs := make(chan Job, N)
	results := make(chan Result, N)

	wp := NewWorkerPool(M, K)
	wp.Start(intakeCtx, jobs, results)

	go producer(intakeCtx, jobs, N)

	for res := range results {
		if res.Err != nil {
			fmt.Printf("Job %d failed: %v\n", res.JobID, res.Err)
		} else {
			fmt.Printf("Job %d succeeded: result=%d\n", res.JobID, res.Result)
		}
	}

	fmt.Printf("Stats: Panics=%d, Failed=%d, Success=%v\n", wp.Stats.Panics, wp.Stats.Failed, wp.Stats.Success)
}
