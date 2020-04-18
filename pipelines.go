package pipelines

import (
	"context"
	"fmt"
	"log"
	"sync"
)

// Processor defines a process' input (Source) channel, its Consumers that care about its Process output, and an Exit handler for when the process is shutdown.
type Processor interface {
	Source() chan interface{}
	Consumers() []Processor
	Process(interface{}) (interface{}, error)
	Exit()
}

// processGroup contains information to run and shutdown a group of Processors
type processGroup struct {
	Ctx    context.Context
	WG     *sync.WaitGroup
	Cancel context.CancelFunc
}

// Pipeline contains a slice of Processors and a grouping to gracefully shutdown all of its Processors
type Pipeline struct {
	Processes []Processor
	groups    []*processGroup
}

// Graph returns a mapping from Processor to its maximum depth in any Process DAG (tree).
// This graph representation of the pipeline is used to a graceful shutdown of every Processor.
func (p *Pipeline) Graph() (map[Processor]int, error) {
	graph := map[Processor]int{}

	for _, proc := range p.Processes {
		visited := map[Processor]bool{}

		err := p.addGraphNode(proc, 0, graph, visited)
		if err != nil {
			return graph, err
		}
	}

	return graph, nil
}

// Run every process in order of its process group. Processes groups
// are assigned to each processes according to its depth in the graph.
// Each process is run in its own go routine.
// An error is returned if there is a cycle detected in the pipeline graph (DAGs).
func (p *Pipeline) Run() error {
	graph, err := p.Graph()

	if err != nil {
		return err
	}

	// find the max depth of the procedure graph
	maxDepth := 0
	for _, d := range graph {
		if d > maxDepth {
			maxDepth = d
		}
	}

	// create run order and validate there are no unknown processes
	// procedure depth
	l := make([][]Processor, maxDepth+1)

	for proc, depth := range graph {
		l[depth] = append(l[depth], proc)
	}

	for _, procs := range l {
		// create procedure group
		pg := newProcessGroup(context.Background())

		// save the pg
		p.groups = append(p.groups, pg)

		pg.WG.Add(len(procs))

		for _, proc := range procs {
			go runProc(pg.Ctx, pg.WG, proc)
		}
	}

	return nil
}

// Shutdown gracefully shutdowns a pipeline in order of proccess groups.
// Root processes will be shutdown first, then their consumers in a BFS order.
func (p *Pipeline) Shutdown() {
	if len(p.groups) == 0 {
		log.Println("Pipeline is not running")
		return
	}
	// cancel and wait on each group in order
	for i, pg := range p.groups {
		log.Printf("Shutting down depth %d processes", i)
		pg.Cancel()
		// Should there be a timeout?
		pg.WG.Wait()
		log.Printf("All depth %d processes shutdown", i)
	}
	log.Println("Shutdown pipeline")
}

// addGraphNode recursively mutates the graph (map[Processor]int) mapping a Processor to its maximum depth
// in a Processes tree (DAG).
func (p *Pipeline) addGraphNode(proc Processor, depth int, graph map[Processor]int, visited map[Processor]bool) error {
	_, isCycle := visited[proc]

	if isCycle {
		return fmt.Errorf("Cycle found for %v: %v %v", proc, graph, visited)
	}

	// mark as visited
	visited[proc] = true

	curDepth, ok := graph[proc]

	// First time visiting, initialize
	// Or we are at a greater depth  than before
	if !ok || depth > curDepth {
		graph[proc] = depth
	}

	for _, c := range proc.Consumers() {
		// copy the visited map for each consumer
		visitCopy := map[Processor]bool{}
		for k, v := range visited {
			visitCopy[k] = v
		}

		err := p.addGraphNode(c, depth+1, graph, visitCopy)
		if err != nil {
			return err
		}
	}
	return nil
}

// runProc runs a Processor Until it receives a cancellation signal from its context.
// While running, a Processor processes all events that comes through its Source and sends the processed output to its consumers.
// On cancellation, the Processor's Exit handler is called before closing its Source.
func runProc(ctx context.Context, wg *sync.WaitGroup, p Processor) {
	defer wg.Done()
	for {
		select {
		case e := <-p.Source():
			out, err := p.Process(e)

			// log and ignore errors
			if err != nil {
				log.Printf("Error processing %v: %v\n", e, err)
				continue
			}

			// implicitly filter nil events
			if out == nil {
				continue
			}

			for _, c := range p.Consumers() {
				c.Source() <- out
			}

		case <-ctx.Done():
			log.Println("Received cancellation signal")
			p.Exit()
			log.Println("Exited")
			close(p.Source())
			log.Println("Closed source channel")
			return
		}
	}
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
