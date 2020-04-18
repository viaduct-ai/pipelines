package pipelines

import (
	"context"
	"fmt"
	"log"
	"sync"
)

// Processor defines a process' input (Source) channel, its process function, and an exit handler.
type Processor interface {
	Source() chan interface{}
	Process(interface{}) (interface{}, error)
	Exit()
}

type processNode struct {
	pipeline  *pipeline
	proc      Processor
	consumers []Processor
	depth     int
}

// Consumer provides read-only access to a pipeline processes consumers
func (node *processNode) Consumers() []Processor {
	return node.consumers
}

// Depth provides read-only access to a pipeline process' graph depth
func (node *processNode) Depth() int {
	return node.depth
}

func (node *processNode) addConsumer(proc Processor) {
	consumers := node.pipeline.Process(proc).consumers
	node.pipeline.Process(proc).consumers = append(consumers, node.proc)
}

func (node *processNode) Consumes(others ...Processor) {
	for _, proc := range others {
		node.addConsumer(proc)
	}
}

type processNodes []*processNode

func (nodes processNodes) Consumes(others ...Processor) {
	// Add p's source channel as a consumer of the other processes
	for _, node := range nodes {
		node.Consumes(others...)
	}
}

// processGroup contains information to run and shutdown a group of Processors
type processGroup struct {
	Ctx    context.Context
	WG     *sync.WaitGroup
	Cancel context.CancelFunc
}

// Pipeline contains a slice of Processors and a grouping to gracefully shutdown all of its Processors
type pipeline struct {
	processes map[Processor]*processNode
	groups    []*processGroup
}

func New() *pipeline {
	processes := map[Processor]*processNode{}

	return &pipeline{
		processes: processes,
	}
}

func (p *pipeline) Process(source Processor) *processNode {
	node, ok := p.processes[source]

	// if it exists, return the pipeline process
	if ok {
		return node
	}

	// else create it
	node = &processNode{
		proc:     source,
		pipeline: p,
	}
	p.processes[source] = node

	return node
}

func (p *pipeline) Processeses(sources ...Processor) processNodes {
	nodes := processNodes{}
	for _, s := range sources {
		nodes = append(nodes, p.Process(s))
	}
	return nodes
}

// Graph returns a mapping from Processor to its maximum depth in any Process DAG (tree).
// This graph representation of the pipeline is used to a graceful shutdown of every Processor.
func (p *pipeline) Graph() (map[Processor]*processNode, error) {
	for proc, _ := range p.processes {
		visited := map[Processor]bool{}

		err := p.addGraphNode(proc, 0, visited)

		if err != nil {
			return p.processes, err
		}
	}

	return p.processes, nil
}

// Run every process in order of its process group. Processes groups
// are assigned to each processes according to its depth in the graph.
// Each process is run in its own go routine.
// An error is returned if there is a cycle detected in the pipeline graph (DAGs).
func (p *pipeline) Run() error {
	graph, err := p.Graph()

	if err != nil {
		return err
	}

	if len(graph) == 0 {
		return fmt.Errorf("no processes to run")
	}

	// find the max depth of the procedure graph
	maxDepth := 0
	for _, node := range graph {
		if node.depth > maxDepth {
			maxDepth = node.depth
		}
	}

	// create run order and validate there are no unknown processes
	// procedure depth
	nodeGroups := make([]processNodes, maxDepth+1)

	for _, node := range graph {
		nodeGroups[node.depth] = append(nodeGroups[node.depth], node)
	}

	for _, nodes := range nodeGroups {
		// create procedure group
		pg := newProcessGroup(context.Background())

		// save the pg
		p.groups = append(p.groups, pg)

		pg.WG.Add(len(nodes))

		for _, node := range nodes {
			go runProc(pg.Ctx, pg.WG, node.proc, node.consumers)
		}
	}

	return nil
}

// Shutdown gracefully shutdowns a pipeline in order of proccess groups.
// Root processes will be shutdown first, then their consumers in a BFS order.
func (p *pipeline) Shutdown() {
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
func (p *pipeline) addGraphNode(proc Processor, depth int, visited map[Processor]bool) error {
	_, isCycle := visited[proc]

	if isCycle {
		return fmt.Errorf("Cycle found for %v: %v %v", proc, p.processes, visited)
	}

	// mark as visited
	visited[proc] = true

	node := p.Process(proc)

	// if we  are at a greater depth than before
	if depth > node.depth {
		node.depth = depth
	}

	for _, c := range node.consumers {
		// copy the visited map for each consumer
		visitCopy := map[Processor]bool{}
		for k, v := range visited {
			visitCopy[k] = v
		}

		err := p.addGraphNode(c, depth+1, visitCopy)
		if err != nil {
			return err
		}
	}
	return nil
}

// runProc runs a Processor Until it receives a cancellation signal from its context.
// While running, a Processor processes all events that comes through its Source and sends the processed output to its consumers.
// On cancellation, the Processor's Exit handler is called before closing its Source.
func runProc(ctx context.Context, wg *sync.WaitGroup, p Processor, consumers []Processor) {
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

			for _, c := range consumers {
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
