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
func New(process func(interface{}) (interface{}, error), exit func()) (pipelines.Processor, error) {
	if process == nil {
		return nil, errors.New("process func is required")
	}

	return &standardProcess{
		source:  make(chan interface{}),
		process: process,
		exit:    exit,
	}, nil
}

// NewTicker decorates a pipelines.Processor by starting a time.Ticker to a passes a value to the Processor's source channel on the given interval. This is often useful for triggering a root pipeline processor on a given interval, such as polling for new events every 10 seconds.
func NewTicker(proc pipelines.Processor, interval time.Duration) (pipelines.Processor, error) {
	if proc == nil || proc.Source() == nil {
		return nil, errors.New("processor must not have a nil Source")
	}
	if interval <= 0 {
		return nil, errors.New("interval must be greater than 0")
	}

	tProc := &tickerProcess{
		proc:   proc,
		ticker: time.NewTicker(interval),
		done:   make(chan bool),
	}
	go tProc.start()

	return tProc, nil
}

type tickerProcess struct {
	proc   pipelines.Processor
	ticker *time.Ticker
	done   chan bool
}

func (p *tickerProcess) start() {
	source := p.proc.Source()
	for {
		select {
		case <-p.done:
			return
		case t := <-p.ticker.C:
			source <- t
		}
	}
}

func (p *tickerProcess) Source() chan interface{} {
	return p.proc.Source()
}

func (p *tickerProcess) Process(i interface{}) (interface{}, error) {
	return p.proc.Process(i)
}

func (p *tickerProcess) Exit() {
	p.done <- true
	p.ticker.Stop()
	p.proc.Exit()
}
