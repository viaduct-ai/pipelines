package processor

import (
	"errors"
	"time"

	"github.com/viaduct-ai/pipelines"
)

type standardProcess struct {
	source  chan interface{}
	process func(interface{}) (interface{}, error)
	exit    func()
}

func (p *standardProcess) Source() chan interface{} {
	return p.source
}

func (p *standardProcess) Process(i interface{}) (interface{}, error) {
	return p.process(i)
}

func (p *standardProcess) Exit() {
	if p.exit == nil {
		return
	}
	p.exit()
}

// New returns a stateless pipelines.Processor. It is intended as a helper for creating functional pipeline Processor's. A process function is required, but the exit function is option.
func New(process func(interface{}) (interface{}, error), exit func(), bufferSize int) (pipelines.Processor, error) {
	if process == nil {
		return nil, errors.New("process func is required")
	}

	return &standardProcess{
		source:  make(chan interface{}, bufferSize),
		process: process,
		exit:    exit,
	}, nil
}

// NewTicker starts a time.Ticker to populate its own source channel on the given interval. This is often useful for triggering a consumer pipeline processor on a given interval, such as polling for new events every 10 seconds.
func NewTicker(interval time.Duration) (pipelines.Processor, error) {
	if interval <= 0 {
		return nil, errors.New("interval must be greater than 0")
	}

	tProc := &tickerProcess{
		source: make(chan interface{}),
		ticker: time.NewTicker(interval),
		done:   make(chan bool),
	}
	go tProc.start()

	return tProc, nil
}

type tickerProcess struct {
	source chan interface{}
	ticker *time.Ticker
	done   chan bool
}

func (p *tickerProcess) start() {
	for {
		select {
		case <-p.done:
			return
		case t := <-p.ticker.C:
			p.source <- t
		}
	}
}

func (p *tickerProcess) Source() chan interface{} {
	return p.source
}

func (p *tickerProcess) Process(i interface{}) (interface{}, error) {
	return i, nil
}

func (p *tickerProcess) Exit() {
	p.done <- true
	p.ticker.Stop()
}
