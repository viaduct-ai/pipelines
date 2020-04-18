package pipelines_test

import (
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/viaduct-ai/pipelines"
)

// create a mock Processor

type Processor struct {
	SourceFunc        func() chan interface{}
	SourceFuncInvoked bool

	ProcessFunc        func(interface{}) (interface{}, error)
	ProcessFuncInvoked bool

	ExitFunc        func()
	ExitFuncInvoked bool
}

func (p *Processor) Source() chan interface{} {
	p.SourceFuncInvoked = true
	return p.SourceFunc()
}

func (p *Processor) Process(i interface{}) (interface{}, error) {
	p.ProcessFuncInvoked = true
	return p.ProcessFunc(i)
}

func (p *Processor) Exit() {
	p.ExitFuncInvoked = true
	p.ExitFunc()
}

func createTestProcesser(source chan interface{}) *Processor {
	return &Processor{
		SourceFunc: func() chan interface{} {
			return source
		},
		ProcessFunc: func(i interface{}) (interface{}, error) {
			switch i.(type) {
			case error:
				return nil, errors.New("error")
			default:
				return i, nil
			}
		},
		ExitFunc: func() {},
	}
}

func TestPipelineGraphSingle(t *testing.T) {
	source := make(chan interface{})

	proc := createTestProcesser(source)

	pipeline := pipelines.New()
	pipeline.Process(proc)

	graph, err := pipeline.Graph()
	assert.NoError(t, err)

	depth, ok := graph[proc]
	assert.True(t, ok, "Graph is missing the processor")
	assert.Equal(t, 0, depth, "Depth for a single node graph should be 0")
}

// A -> B -> C -> D
func TestPipelineGraphSimple(t *testing.T) {
	sourceA := make(chan interface{})
	sourceB := make(chan interface{})
	sourceC := make(chan interface{})
	sourceD := make(chan interface{})

	procD := createTestProcesser(sourceD)
	procC := createTestProcesser(sourceC)
	procB := createTestProcesser(sourceB)
	procA := createTestProcesser(sourceA)

	pipeline := pipelines.New()

	pipeline.Process(procB).Consumes(procA)
	pipeline.Process(procC).Consumes(procB)
	pipeline.Process(procD).Consumes(procC)

	expected := map[pipelines.Processor]int{
		procA: 0,
		procB: 1,
		procC: 2,
		procD: 3,
	}
	graph, err := pipeline.Graph()
	assert.NoError(t, err)

	t.Log(graph)
	for k, v := range graph {
		expect, ok := expected[k]
		assert.Truef(t, ok, "Unknown processor %q", k)
		assert.Equalf(t, expect, v, "Values do not match. Got %q but expected %q at key %q", v, expect, k)
	}
}

// A -> B -> C  D -> C
func TestPipelineGraphComplex(t *testing.T) {
	sourceA := make(chan interface{})
	sourceB := make(chan interface{})
	sourceC := make(chan interface{})
	sourceD := make(chan interface{})

	procC := createTestProcesser(sourceC)
	procD := createTestProcesser(sourceD)

	procB := createTestProcesser(sourceB)
	procA := createTestProcesser(sourceA)

	pipeline := pipelines.New()

	pipeline.Process(procB).Consumes(procA)
	pipeline.Process(procC).Consumes(procB)
	pipeline.Process(procC).Consumes(procD)

	expected := map[pipelines.Processor]int{
		procA: 0,
		procB: 1,
		procC: 2,
		procD: 0,
	}
	graph, err := pipeline.Graph()
	assert.NoError(t, err)

	t.Log(graph)
	for k, v := range graph {
		expect, ok := expected[k]
		assert.Truef(t, ok, "Unknown processor %v", k)
		assert.Equalf(t, expect, v, "Values do not match. Got %v but expected %v at key %v", v, expect, k)
	}
}

// A -> B -> C -> B
func TestPipelineGraphCycle(t *testing.T) {
	sourceA := make(chan interface{})
	sourceB := make(chan interface{})
	sourceC := make(chan interface{})

	procC := createTestProcesser(sourceC)
	procB := createTestProcesser(sourceB)
	procA := createTestProcesser(sourceA)

	pipeline := pipelines.New()

	pipeline.Process(procB).Consumes(procA)
	pipeline.Process(procC).Consumes(procB)
	pipeline.Process(procB).Consumes(procC)

	g, err := pipeline.Graph()
	t.Log(g)
	assert.Error(t, err, "Graph should fail on a cycle")
}

func TestPipelineRunEmpty(t *testing.T) {
	pipeline := pipelines.New()

	err := pipeline.Run()
	assert.Error(t, err)
}

func TestPipelineRunSingleProccessor(t *testing.T) {
	source := make(chan interface{})

	proc := createTestProcesser(source)

	pipeline := pipelines.New()
	pipeline.Process(proc).Consumes()

	err := pipeline.Run()
	assert.NoError(t, err)

	assert.False(t, proc.ProcessFuncInvoked)
	source <- "test"
	assert.Eventuallyf(t, func() bool {
		return proc.ProcessFuncInvoked
	}, time.Second, 10*time.Millisecond, "Proccess function was not invoked")
}

// A -> B -> C  D -> C
func TestPipelineRunManyProccessors(t *testing.T) {
	sourceA := make(chan interface{})
	sourceB := make(chan interface{})
	sourceC := make(chan interface{})
	sourceD := make(chan interface{})

	procC := createTestProcesser(sourceC)
	procD := createTestProcesser(sourceD)

	procB := createTestProcesser(sourceB)
	procA := createTestProcesser(sourceA)

	pipeline := pipelines.New()

	pipeline.Process(procB).Consumes(procA)
	pipeline.Process(procC).Consumes(procB)
	pipeline.Process(procC).Consumes(procD)

	err := pipeline.Run()
	assert.NoError(t, err)

	procs := []*Processor{procA, procB, procC, procD}
	for i, p := range procs {
		assert.Falsef(t, p.ProcessFuncInvoked, "Failed on process %d", i)
	}

	// send a event through one of the pipeline dags
	sourceA <- "test"
	dagA := []*Processor{procA, procB, procC}
	for i, p := range dagA {
		assert.Eventuallyf(t, func() bool {
			return p.ProcessFuncInvoked
		}, time.Second, 10*time.Millisecond, "Failed on process %d", i)
		assert.Truef(t, p.ProcessFuncInvoked, "Failed on process %d", i)
	}
	// explicitly reset procC false
	procC.ProcessFuncInvoked = false

	// send a event through the other pipeline dag
	sourceD <- "test"
	dagB := []*Processor{procD, procC}
	for i, p := range dagB {
		assert.Eventuallyf(t, func() bool {
			return p.ProcessFuncInvoked
		}, time.Second, 10*time.Millisecond, "Failed on process %d", i)
	}
}

func TestPipelineShutdownsSingleProcessor(t *testing.T) {
	source := make(chan interface{})

	proc := createTestProcesser(source)

	pipeline := pipelines.New()
	pipeline.Process(proc).Consumes()

	err := pipeline.Run()
	assert.NoError(t, err)

	assert.False(t, proc.ExitFuncInvoked)
	pipeline.Shutdown()
	assert.True(t, proc.ExitFuncInvoked)
}

// A -> B -> C  D -> C
func TestPipelineShutdownsManyProcessors(t *testing.T) {
	sourceA := make(chan interface{})
	sourceB := make(chan interface{})
	sourceC := make(chan interface{})
	sourceD := make(chan interface{})

	procC := createTestProcesser(sourceC)
	procD := createTestProcesser(sourceD)

	procB := createTestProcesser(sourceB)
	procA := createTestProcesser(sourceA)

	pipeline := pipelines.New()
	pipeline.Process(procB).Consumes(procA)
	pipeline.Process(procC).Consumes(procB)
	pipeline.Process(procC).Consumes(procD)

	err := pipeline.Run()
	assert.NoError(t, err)

	procs := []*Processor{procA, procB, procC, procD}
	for i, p := range procs {
		assert.Falsef(t, p.ExitFuncInvoked, "Failed on process %d", i)
	}

	pipeline.Shutdown()

	for i, p := range procs {
		assert.Truef(t, p.ExitFuncInvoked, "Failed on process %d", i)
	}

}

func TestPipelineRunIgnoresEmptyEvents(t *testing.T) {
	sourceA := make(chan interface{})
	sourceB := make(chan interface{})
	sourceC := make(chan interface{})
	sourceD := make(chan interface{})

	procD := createTestProcesser(sourceD)
	procC := createTestProcesser(sourceC)
	procB := createTestProcesser(sourceB)
	procA := createTestProcesser(sourceA)

	pipeline := pipelines.New()
	pipeline.Process(procB).Consumes(procA)
	pipeline.Process(procC).Consumes(procB)
	pipeline.Process(procD).Consumes(procC)

	err := pipeline.Run()
	assert.NoError(t, err)

	procA.Source() <- nil
	assert.Eventuallyf(t, func() bool {
		return procA.ProcessFuncInvoked
	}, time.Second, 10*time.Millisecond, "Proccess A function was not invoked")

	// give enough time for data to flow through
	time.Sleep(time.Millisecond * 100)

	for i, p := range []*Processor{procB, procC, procD} {
		assert.Falsef(t, p.ProcessFuncInvoked, "Processor %d process function should not be called on nil", i)
	}
	pipeline.Shutdown()
}

func TestPipelineRunIgnoresErrors(t *testing.T) {
	sourceA := make(chan interface{})
	sourceB := make(chan interface{})
	sourceC := make(chan interface{})
	sourceD := make(chan interface{})

	procD := createTestProcesser(sourceD)
	procC := createTestProcesser(sourceC)
	procB := createTestProcesser(sourceB)
	procA := createTestProcesser(sourceA)

	pipeline := pipelines.New()

	pipeline.Process(procB).Consumes(procA)
	pipeline.Process(procC).Consumes(procB)
	pipeline.Process(procD).Consumes(procC)

	err := pipeline.Run()
	assert.NoError(t, err)

	procA.Source() <- errors.New("error")

	assert.Eventuallyf(t, func() bool {
		return procA.ProcessFuncInvoked
	}, time.Second, 10*time.Millisecond, "Proccess A function was not invoked")

	// give enough time for data to flow through
	time.Sleep(time.Millisecond * 100)

	for i, p := range []*Processor{procB, procC, procD} {
		assert.Falsef(t, p.ProcessFuncInvoked, "Processor %d process function should not be called on nil", i)
	}
	pipeline.Shutdown()
}

// benchmark for fun?