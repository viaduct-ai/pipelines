package pipelines

import (
	"context"
	"sync"
)

// processGroup contains information to run and shutdown a group of Processors
type processGroup struct {
	Ctx    context.Context
	WG     *sync.WaitGroup
	Cancel context.CancelFunc
}

// newProcessGroup returns a new process group from a parent context
func newProcessGroup(parent context.Context) *processGroup {
	ctx, cancel := context.WithCancel(parent)
	wg := &sync.WaitGroup{}
	return &processGroup{
		Ctx:    ctx,
		Cancel: cancel,
		WG:     wg,
	}
}
