package pipelines

// ProcessNode represents a node of the pipeline graph. It relates a Processor to its consumers and depth in a graph with respect to a Pipeline. A ProcessNode's referent to its pipeline allows for the synatic sugar of pipeline.Process(...).Consumers(...).
type ProcessNode struct {
	pipeline  *Pipeline
	proc      Processor
	consumers []Processor
	depth     int
}

// Consumers provides read-only access to a pipeline processes consumers.
func (node *ProcessNode) Consumers() []Processor {
	return node.consumers
}

// Depth provides read-only access to a pipeline process' graph depth.
func (node *ProcessNode) Depth() int {
	return node.depth
}

// addAsConsumer adds the process node as a consumer to another processor.
func (node *ProcessNode) addAsConsumer(proc Processor) {
	consumers := node.pipeline.Process(proc).consumers
	node.pipeline.Process(proc).consumers = append(consumers, node.proc)
}

// Consumes adds the node Processor as a consumer of other Processors.
func (node *ProcessNode) Consumes(others ...Processor) {
	for _, proc := range others {
		node.addAsConsumer(proc)
	}
}

// ProcessNodes allows of operations on slices of ProcessNodes.
type ProcessNodes []*ProcessNode

// Consumes adds all the process nodes as consumers of the other Processors.
func (nodes ProcessNodes) Consumes(others ...Processor) {
	// Add p's source channel as a consumer of the other processes
	for _, node := range nodes {
		node.Consumes(others...)
	}
}
