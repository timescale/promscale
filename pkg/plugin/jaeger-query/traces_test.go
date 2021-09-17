package jaeger_query

import (
	"fmt"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestPrepareTrace(t *testing.T) {
	trace := prepareDemoTrace()
	jaegerTrace, err := toJaeger(trace)
	require.NoError(t, err)
	fmt.Println(jaegerTrace)
	fmt.Println(jaegerTrace[0].Process)
	fmt.Println("above this")
	fmt.Println(len(jaegerTrace[0].Spans))
	fmt.Println(jaegerTrace[0].Spans)
	fmt.Println(jaegerTrace[0].Spans[0].StartTime)
	fmt.Println(jaegerTrace[0].Spans[0].Duration)
}
