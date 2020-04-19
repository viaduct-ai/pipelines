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

// Pipeline contains a mapping of Processors to its graph metadata and a grouping to gracefully shutdown all of its Processors.
type Pipeline struct {
	processes map[Processor]*ProcessNode
	groups    []*processGroup
}

// New initialize an empty pipeline. A pipeline should always be initialized with this function.
func New() *Pipeline {
	processes := map[Processor]*ProcessNode{}

	return &Pipeline{
		processes: processes,
	}
}

// Process returns a *ProcessNode for a given Processor. If the Processor doesn't exist in the Pipeline, a new node will be created for it and returned.
func (p *Pipeline) Process(source Processor) *ProcessNode {
	node, ok := p.processes[source]

	// if it exists, return the pipeline process
	if ok {
		return node
	}

	// else create it
	node = &ProcessNode{
		proc:     source,
		pipeline: p,
	}
	p.processes[source] = node

	return node
}

// Processes returns ProcessNodes for all inputted Processors. If any of the Processor don't exist in the Pipeline, a new node will be created for it and returned.
func (p *Pipeline) Processes(sources ...Processor) ProcessNodes {
	nodes := ProcessNodes{}
	for _, s := range sources {
		nodes = append(nodes, p.Process(s))
	}
	return nodes
}

// Graph returns a mapping from Processor to its ProcessNode metadata.
// This graph representation of the pipeline is used to:
// 1. Maintain the relationships between Processor and the other Processors that care about their output (consumers).
// 2. Gracefully shutdown of every Processor.
func (p *Pipeline) Graph() (map[Processor]*ProcessNode, error) {
	for proc := range p.processes {
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
func (p *Pipeline) Run() error {
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
	nodeGroups := make([]ProcessNodes, maxDepth+1)

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
func (p *Pipeline) addGraphNode(proc Processor, depth int, visited map[Processor]bool) error {
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

	// initialize before the for select
	source := p.Source()
	consumerChans := make([]chan interface{}, len(consumers))
	for _, c := range consumers {
		consumerChans = append(consumerChans, c.Source())
	}

	for {
		select {
		case e := <-source:
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

			for _, c := range consumerChans {
				c <- out
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
