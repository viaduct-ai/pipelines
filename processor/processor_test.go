package processor_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/viaduct-ai/pipelines"
	"github.com/viaduct-ai/pipelines/processor"
)

type testProcess struct {
	ProcessFuncInvoked     bool
	ProcessFuncInvokedWith interface{}

	ExitFuncInvoked bool
}

func (p *testProcess) process(i interface{}) (interface{}, error) {
	p.ProcessFuncInvoked = true
	p.ProcessFuncInvokedWith = i
	return i, nil
}

func (p *testProcess) exit() {
	p.ExitFuncInvoked = true
}

func TestNewProcessorRequireProcessFunc(t *testing.T) {
	_, err := processor.New(nil, nil)
	assert.Error(t, err)
}

func TestNewProcessor(t *testing.T) {
	test := &testProcess{}
	expected := "test"

	proc, err := processor.New(test.process, test.exit)
	assert.NoError(t, err)

	pipeline := pipelines.New()

	pipeline.Process(proc)

	err = pipeline.Run()
	assert.NoError(t, err)
	assert.False(t, test.ProcessFuncInvoked)

	source := proc.Source()
	source <- expected

	pipeline.Shutdown()

	assert.True(t, test.ProcessFuncInvoked)
	assert.Equal(t, expected, test.ProcessFuncInvokedWith)
	assert.True(t, test.ExitFuncInvoked)
}

func TestNewTickerProcessorRequiresPositiveInterval(t *testing.T) {
	_, err := processor.NewTicker(-1 * time.Millisecond)
	assert.Error(t, err)

	_, err = processor.NewTicker(0 * time.Millisecond)
	assert.Error(t, err)
}

func TestNewTickerProcessor(t *testing.T) {
	test := &testProcess{}
	interval := time.Millisecond

	proc, err := processor.New(test.process, test.exit)
	assert.NoError(t, err)

	tickerProc, err := processor.NewTicker(interval)
	assert.NoError(t, err)

	pipeline := pipelines.New()

	pipeline.Process(proc).Consumes(tickerProc)

	err = pipeline.Run()
	assert.NoError(t, err)

	// sleep the same amount as the interval
	time.Sleep(interval)

	pipeline.Shutdown()

	assert.True(t, test.ProcessFuncInvoked)
	assert.True(t, test.ExitFuncInvoked)
}
