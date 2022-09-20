package end_to_end_tests

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/gogo/protobuf/jsonpb"
	"github.com/gogo/protobuf/proto"
	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/jaegertracing/jaeger/model"
	"github.com/jaegertracing/jaeger/storage/spanstore"
	jaegertranslator "github.com/open-telemetry/opentelemetry-collector-contrib/pkg/translator/jaeger"
	"github.com/stretchr/testify/require"
	"github.com/timescale/promscale/pkg/jaeger/store"
	jaegerstore "github.com/timescale/promscale/pkg/jaeger/store"
	ingstr "github.com/timescale/promscale/pkg/pgmodel/ingestor"
	"github.com/timescale/promscale/pkg/pgxconn"
	"github.com/timescale/promscale/pkg/tests/testdata"
)

// Similar to TestQueryTraces, but uses Jaeger span ingestion interface.
func TestJaegerSpanIngestion(t *testing.T) {
	withDB(t, "jaeger_span_store_e2e", func(db *pgxpool.Pool, t testing.TB) {
		ingestor, err := ingstr.NewPgxIngestorForTests(pgxconn.NewPgxConn(db), nil)
		require.NoError(t, err)
		defer ingestor.Close()

		jaegerStore := jaegerstore.New(pgxconn.NewQueryLoggingPgxConn(db), ingestor, &store.DefaultConfig)

		batch, err := jaegertranslator.ProtoFromTraces(testdata.GenerateTestTrace())
		require.NoError(t, err)
		for _, b := range batch {
			for _, s := range b.Spans {
				// ProtoFromTraces doesn't populates span.Process because it is already been exposed by batch.Process.
				// See https://github.com/jaegertracing/jaeger-idl/blob/05fe64e9c305526901f70ff692030b388787e388/proto/api_v2/model.proto#L152-L160
				s.Process = b.Process
				err = jaegerStore.SpanWriter().WriteSpan(context.Background(), s)
				require.NoError(t, err)
			}
		}
		getOperationsTest(t, jaegerStore)
		findTraceTest(t, jaegerStore)
		getDependenciesTest(t, jaegerStore)
	})
}

// This is the same test done by Jaeger in their gRPC plugin test case 'Tags in
// one spot - Tags' for the integration tests `FindTraces`.
func TestJaegerSpanBinarayTag(t *testing.T) {
	withDB(t, "jaeger_span_store_e2e", func(db *pgxpool.Pool, t testing.TB) {
		ingestor, err := ingstr.NewPgxIngestorForTests(pgxconn.NewPgxConn(db), nil)
		require.NoError(t, err)
		defer ingestor.Close()

		jaegerStore := jaegerstore.New(pgxconn.NewQueryLoggingPgxConn(db), ingestor, &store.DefaultConfig)

		trace := getTraceFixtureExact(t, "./fixtures/jaeger_binaray_tag/trace.json")
		err = jaegerStore.WriteSpan(context.Background(), trace.GetSpans()[0])
		require.NoError(t, err)
		query := loadAndParseQuery(t, "./fixtures/jaeger_binaray_tag/query.json")
		traces, err := jaegerStore.FindTraces(context.Background(), query)
		require.NoError(t, err)

		trace = getTraceFixtureExact(t, "./fixtures/jaeger_binaray_tag/trace.json")
		CompareTraces(t, trace, traces[0])
	})
}

func TestJaeger(t *testing.T) {
	withDB(t, "jaeger_span_store_e2e", func(db *pgxpool.Pool, t testing.TB) {
		ingestor, err := ingstr.NewPgxIngestorForTests(pgxconn.NewPgxConn(db), nil)
		require.NoError(t, err)
		defer ingestor.Close()

		jaegerStore := jaegerstore.New(pgxconn.NewQueryLoggingPgxConn(db), ingestor, &store.DefaultConfig)

		trace := getTraceFixtureExact(t, "./fixtures/tags_plus_operation_name/trace.json")
		err = jaegerStore.WriteSpan(context.Background(), trace.GetSpans()[0])
		require.NoError(t, err)
		query := loadAndParseQuery(t, "./fixtures/tags_plus_operation_name/query.json")
		traces, err := jaegerStore.FindTraces(context.Background(), query)
		require.NoError(t, err)

		trace = getTraceFixtureExact(t, "./fixtures/tags_plus_operation_name/trace.json")
		CompareTraces(t, trace, traces[0])
	})
}

func loadAndParseJSONPB(t testing.TB, path string, object proto.Message) {
	// #nosec
	inStr, err := os.ReadFile(path)
	require.NoError(t, err, "Not expecting error when loading fixture %s", path)
	err = jsonpb.Unmarshal(bytes.NewReader(correctTime(inStr)), object)
	require.NoError(t, err, "Not expecting error when unmarshaling fixture %s", path)
}

// required, because we want to only query on recent traces, so we replace all the dates with recent dates.
func correctTime(json []byte) []byte {
	jsonString := string(json)
	now := time.Now().UTC()
	yesterday := now.AddDate(0, 0, -1).Format("2006-01-02")
	twoDaysAgo := now.AddDate(0, 0, -2).Format("2006-01-02")
	retString := strings.Replace(jsonString, "2017-01-26", yesterday, -1)
	retString = strings.Replace(retString, "2017-01-25", twoDaysAgo, -1)
	return []byte(retString)
}

func getTraceFixtureExact(t testing.TB, fileName string) *model.Trace {
	var trace model.Trace
	loadAndParseJSONPB(t, fileName, &trace)
	return &trace
}

func loadAndParseQuery(t testing.TB, path string) *spanstore.TraceQueryParameters {
	inStr, err := os.ReadFile(path)
	var query spanstore.TraceQueryParameters
	require.NoError(t, err, "Not expecting error when loading fixture %s", path)
	err = json.Unmarshal(correctTime(inStr), &query)
	require.NoError(t, err, "Not expecting error when unmarshaling fixture %s", path)
	return &query
}
