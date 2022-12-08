package loki

import (
	"bufio"
	"encoding/json"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestLokiFramer(t *testing.T) {
	t.Run("converting metric name", func(t *testing.T) {
		msg := []byte(`{"streams":[
			{"stream":
			  {"job":"node-exporter","metric":"go_memstats_heap_inuse_bytes"},
			  "values":[
				["1642091525267322910","line1"]
			  ]},
			{"stream":
			  {"job":"node-exporter","metric":"go_memstats_heap_inuse_bytes"},
			  "values":[
				  ["1642091525770585774","line2"],
				  ["1642091525770585775","line3"]
			  ]},
			{"stream":
			  {"metric":"go_memstats_heap_inuse_bytes","job":"node-exporter"},
			  "values":[
				  ["1642091526263785281","line4"]
			   ]}
			]}`)

		frame, err := lokiBytesToLabeledFrame(msg)
		require.NoError(t, err)

		lines := frame.Fields[2]
		require.Equal(t, 4, lines.Len())
		require.Equal(t, "line1", lines.At(0))
		require.Equal(t, "line2", lines.At(1))
		require.Equal(t, "line3", lines.At(2))
		require.Equal(t, "line4", lines.At(3))
	})
}

const fixture = `data: {"status":"success","data":{"resultType":"streams","result":[1]}}


data: {"status":"success","data":{"resultType":"streams","result":[2]}}


data: {"status":"success","data":{"resultType":"streams","result":[3]}}`

type testData struct {
	Status string
	Data struct {
		ResultType string
		Result []int
	}
}

func TestLokiEventStream(t *testing.T) {
	blob := `{"status":"success","data":{"resultType":"streams","result":[5]}}`
	var d testData 
	err := json.Unmarshal([]byte(blob), &d)
	require.NoError(t, err)
	require.Equal(t, 5, d.Data.Result[0])

	scanner := bufio.NewScanner(strings.NewReader(fixture))
	scanner.Split(eventStream)
	count := 0	
	for scanner.Scan() {
		count++
		blob := scanner.Bytes()
		var d testData 
		err := json.Unmarshal(blob, &d)
		require.NoErrorf(t, err, "JSON: %s, iteraton: %d", string(blob), count)
		require.Equalf(t, count, d.Data.Result[0], "JSON: %s", string(blob))
	}
	require.Equal(t, 3, count)
}

