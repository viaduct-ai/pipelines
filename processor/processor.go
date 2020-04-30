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

func NewTicker(proc pipelines.Processor, interval time.Duration) (pipelines.Processor, error) {
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
