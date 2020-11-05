package end_to_end_tests

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"reflect"
	"sort"
	"testing"
	"time"

	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/timescale/promscale/pkg/internal/testhelpers"
)

const (
	valueDiffThreshold = 0.1
)

type queryResponse struct {
	Status   string    `json:"status"`
	Data     queryData `json:"data,omitempty"`
	Warnings []string  `json:"warnings,omitempty"`
}

func (q queryResponse) Equal(other queryResponse) bool {
	if q.Status != other.Status {
		return false
	}

	if !reflect.DeepEqual(q.Warnings, other.Warnings) {
		return false
	}

	return q.Data.Equal(other.Data)
}

type queryData struct {
	ResultType string  `json:"resultType"`
	Result     samples `json:"result"`
}

func (q queryData) Equal(other queryData) bool {
	if q.ResultType != other.ResultType {
		return false
	}

	return q.Result.Equal(other.Result)
}

func genInstantRequest(apiURL, query string, start time.Time) (*http.Request, error) {
	u, err := url.Parse(fmt.Sprintf("%s/query", apiURL))

	if err != nil {
		return nil, err
	}

	val := url.Values{}

	val.Add("query", query)
	val.Add("time", fmt.Sprintf("%d", start.Unix()))

	u.RawQuery = val.Encode()

	return http.NewRequest(
		"GET",
		u.String(),
		nil,
	)
}

func genRangeRequest(apiURL, query string, start, end time.Time, step time.Duration) (*http.Request, error) {
	u, err := url.Parse(fmt.Sprintf("%s/query_range", apiURL))

	if err != nil {
		return nil, err
	}

	val := url.Values{}

	val.Add("query", query)
	val.Add("start", fmt.Sprintf("%d", start.Unix()))
	val.Add("end", fmt.Sprintf("%d", end.Unix()))
	val.Add("step", fmt.Sprintf("%f", step.Seconds()))

	u.RawQuery = val.Encode()

	return http.NewRequest(
		"GET",
		u.String(),
		nil,
	)
}

func TestPromQLQueryEndpointRealDataset(t *testing.T) {
	if testing.Short() || !*extendedTest {
		t.Skip("skipping integration test")
	}

	testCases := []testCase{
		{
			name:  "real query 1",
			query: `42`,
		},
		{
			name:  "real query 2",
			query: `1.234`,
		},
		{
			name:  "real query 3",
			query: `.123e-9`,
		},
		{
			name:  "real query 4",
			query: `0x3d`,
		},
		{
			name:  "real query 5",
			query: `Inf`,
		},
		{
			name:  "real query 6",
			query: `+Inf`,
		},
		{
			name:  "real query 7",
			query: `-Inf`,
		},
		{
			name:  "real query 8",
			query: `NaN`,
		},
		{
			name:  "real query 9",
			query: `demo_cpu_usage_seconds_total`,
		},
		{
			name:  "real query 10",
			query: `{__name__="demo_cpu_usage_seconds_total"}`,
		},
		{
			name:  "real query 11",
			query: `demo_cpu_usage_seconds_total{mode="idle"}`,
		},
		{
			name:  "real query 12",
			query: `demo_cpu_usage_seconds_total{mode!="idle"}`,
		},
		{
			name:  "real query 13",
			query: `demo_cpu_usage_seconds_total{instance=~"demo.promlabs.com:.*"}`,
		},
		{
			name:  "real query 14",
			query: `demo_cpu_usage_seconds_total{instance=~"host"}`,
		},
		{
			name:  "real query 15",
			query: `demo_cpu_usage_seconds_total{instance!~".*:10000"}`,
		},
		{
			name:  "real query 16",
			query: `demo_cpu_usage_seconds_total{mode="idle", instance!="demo.promlabs.com:10000"}`,
		},
		{
			name:  "real query 17",
			query: `{mode="idle", instance!="demo.promlabs.com:10000"}`,
		},
		{
			name:  "real query 18",
			query: `{__name__=~".*"}`,
		},
		{
			name:  "real query 19",
			query: `nonexistent_metric_name`,
		},
		{
			name:  "real query 20",
			query: `demo_cpu_usage_seconds_total offset 1m`,
		},
		{
			name:  "real query 21",
			query: `demo_cpu_usage_seconds_total offset 5m`,
		},
		{
			name:  "real query 22",
			query: `demo_cpu_usage_seconds_total offset 10m`,
		},
		{
			name:  "real query 23",
			query: `demo_cpu_usage_seconds_total offset -1m`,
		},
		{
			name:  "real query 24",
			query: `demo_cpu_usage_seconds_total offset -5m`,
		},
		{
			name:  "real query 25",
			query: `demo_cpu_usage_seconds_total offset -10m`,
		},
		{
			name:  "real query 26",
			query: `sum(demo_cpu_usage_seconds_total)`,
		},
		{
			name:  "real query 27",
			query: `avg(demo_cpu_usage_seconds_total)`,
		},
		{
			name:  "real query 28",
			query: `max(demo_cpu_usage_seconds_total)`,
		},
		{
			name:  "real query 29",
			query: `min(demo_cpu_usage_seconds_total)`,
		},
		{
			name:  "real query 30",
			query: `count(demo_cpu_usage_seconds_total)`,
		},
		{
			name:  "real query 31",
			query: `stddev(demo_cpu_usage_seconds_total)`,
		},
		{
			name:  "real query 32",
			query: `stdvar(demo_cpu_usage_seconds_total)`,
		},
		{
			name:  "real query 33",
			query: `sum(nonexistent_metric_name)`,
		},
		{
			name:  "real query 34",
			query: `avg(nonexistent_metric_name)`,
		},
		{
			name:  "real query 35",
			query: `max(nonexistent_metric_name)`,
		},
		{
			name:  "real query 36",
			query: `min(nonexistent_metric_name)`,
		},
		{
			name:  "real query 37",
			query: `count(nonexistent_metric_name)`,
		},
		{
			name:  "real query 38",
			query: `stddev(nonexistent_metric_name)`,
		},
		{
			name:  "real query 39",
			query: `stdvar(nonexistent_metric_name)`,
		},
		{
			name:  "real query 40",
			query: `sum by() (demo_cpu_usage_seconds_total)`,
		},
		{
			name:  "real query 41",
			query: `avg by() (demo_cpu_usage_seconds_total)`,
		},
		{
			name:  "real query 42",
			query: `max by() (demo_cpu_usage_seconds_total)`,
		},
		{
			name:  "real query 43",
			query: `min by() (demo_cpu_usage_seconds_total)`,
		},
		{
			name:  "real query 44",
			query: `count by() (demo_cpu_usage_seconds_total)`,
		},
		{
			name:  "real query 45",
			query: `stddev by() (demo_cpu_usage_seconds_total)`,
		},
		{
			name:  "real query 46",
			query: `stdvar by() (demo_cpu_usage_seconds_total)`,
		},
		{
			name:  "real query 47",
			query: `sum by(instance) (demo_cpu_usage_seconds_total)`,
		},
		{
			name:  "real query 48",
			query: `avg by(instance) (demo_cpu_usage_seconds_total)`,
		},
		{
			name:  "real query 49",
			query: `max by(instance) (demo_cpu_usage_seconds_total)`,
		},
		{
			name:  "real query 50",
			query: `min by(instance) (demo_cpu_usage_seconds_total)`,
		},
		{
			name:  "real query 51",
			query: `count by(instance) (demo_cpu_usage_seconds_total)`,
		},
		{
			name:  "real query 52",
			query: `stddev by(instance) (demo_cpu_usage_seconds_total)`,
		},
		{
			name:  "real query 53",
			query: `stdvar by(instance) (demo_cpu_usage_seconds_total)`,
		},
		{
			name:  "real query 54",
			query: `sum by(instance, mode) (demo_cpu_usage_seconds_total)`,
		},
		{
			name:  "real query 55",
			query: `avg by(instance, mode) (demo_cpu_usage_seconds_total)`,
		},
		{
			name:  "real query 56",
			query: `max by(instance, mode) (demo_cpu_usage_seconds_total)`,
		},
		{
			name:  "real query 57",
			query: `min by(instance, mode) (demo_cpu_usage_seconds_total)`,
		},
		{
			name:  "real query 58",
			query: `count by(instance, mode) (demo_cpu_usage_seconds_total)`,
		},
		{
			name:  "real query 59",
			query: `stddev by(instance, mode) (demo_cpu_usage_seconds_total)`,
		},
		{
			name:  "real query 60",
			query: `stdvar by(instance, mode) (demo_cpu_usage_seconds_total)`,
		},
		{
			name:  "real query 61",
			query: `sum by(nonexistent) (demo_cpu_usage_seconds_total)`,
		},
		{
			name:  "real query 62",
			query: `avg by(nonexistent) (demo_cpu_usage_seconds_total)`,
		},
		{
			name:  "real query 63",
			query: `max by(nonexistent) (demo_cpu_usage_seconds_total)`,
		},
		{
			name:  "real query 64",
			query: `min by(nonexistent) (demo_cpu_usage_seconds_total)`,
		},
		{
			name:  "real query 65",
			query: `count by(nonexistent) (demo_cpu_usage_seconds_total)`,
		},
		{
			name:  "real query 66",
			query: `stddev by(nonexistent) (demo_cpu_usage_seconds_total)`,
		},
		{
			name:  "real query 67",
			query: `stdvar by(nonexistent) (demo_cpu_usage_seconds_total)`,
		},
		{
			name:  "real query 68",
			query: `sum without() (demo_cpu_usage_seconds_total)`,
		},
		{
			name:  "real query 69",
			query: `avg without() (demo_cpu_usage_seconds_total)`,
		},
		{
			name:  "real query 70",
			query: `max without() (demo_cpu_usage_seconds_total)`,
		},
		{
			name:  "real query 71",
			query: `min without() (demo_cpu_usage_seconds_total)`,
		},
		{
			name:  "real query 72",
			query: `count without() (demo_cpu_usage_seconds_total)`,
		},
		{
			name:  "real query 73",
			query: `stddev without() (demo_cpu_usage_seconds_total)`,
		},
		{
			name:  "real query 74",
			query: `stdvar without() (demo_cpu_usage_seconds_total)`,
		},
		{
			name:  "real query 75",
			query: `sum without(instance) (demo_cpu_usage_seconds_total)`,
		},
		{
			name:  "real query 76",
			query: `avg without(instance) (demo_cpu_usage_seconds_total)`,
		},
		{
			name:  "real query 77",
			query: `max without(instance) (demo_cpu_usage_seconds_total)`,
		},
		{
			name:  "real query 78",
			query: `min without(instance) (demo_cpu_usage_seconds_total)`,
		},
		{
			name:  "real query 79",
			query: `count without(instance) (demo_cpu_usage_seconds_total)`,
		},
		{
			name:  "real query 80",
			query: `stddev without(instance) (demo_cpu_usage_seconds_total)`,
		},
		{
			name:  "real query 81",
			query: `stdvar without(instance) (demo_cpu_usage_seconds_total)`,
		},
		{
			name:  "real query 82",
			query: `sum without(instance, mode) (demo_cpu_usage_seconds_total)`,
		},
		{
			name:  "real query 83",
			query: `avg without(instance, mode) (demo_cpu_usage_seconds_total)`,
		},
		{
			name:  "real query 84",
			query: `max without(instance, mode) (demo_cpu_usage_seconds_total)`,
		},
		{
			name:  "real query 85",
			query: `min without(instance, mode) (demo_cpu_usage_seconds_total)`,
		},
		{
			name:  "real query 86",
			query: `count without(instance, mode) (demo_cpu_usage_seconds_total)`,
		},
		{
			name:  "real query 87",
			query: `stddev without(instance, mode) (demo_cpu_usage_seconds_total)`,
		},
		{
			name:  "real query 88",
			query: `stdvar without(instance, mode) (demo_cpu_usage_seconds_total)`,
		},
		{
			name:  "real query 89",
			query: `sum without(nonexistent) (demo_cpu_usage_seconds_total)`,
		},
		{
			name:  "real query 90",
			query: `avg without(nonexistent) (demo_cpu_usage_seconds_total)`,
		},
		{
			name:  "real query 91",
			query: `max without(nonexistent) (demo_cpu_usage_seconds_total)`,
		},
		{
			name:  "real query 92",
			query: `min without(nonexistent) (demo_cpu_usage_seconds_total)`,
		},
		{
			name:  "real query 93",
			query: `count without(nonexistent) (demo_cpu_usage_seconds_total)`,
		},
		{
			name:  "real query 94",
			query: `stddev without(nonexistent) (demo_cpu_usage_seconds_total)`,
		},
		{
			name:  "real query 95",
			query: `stdvar without(nonexistent) (demo_cpu_usage_seconds_total)`,
		},
		{
			name:  "real query 96",
			query: `topk (3, demo_cpu_usage_seconds_total)`,
		},
		{
			name:  "real query 97",
			query: `bottomk (3, demo_cpu_usage_seconds_total)`,
		},
		{
			name:  "real query 98",
			query: `topk by(instance) (2, demo_cpu_usage_seconds_total)`,
		},
		{
			name:  "real query 99",
			query: `bottomk by(instance) (2, demo_cpu_usage_seconds_total)`,
		},
		{
			name:  "real query 100",
			query: `quantile(-0.5, demo_cpu_usage_seconds_total)`,
		},
		{
			name:  "real query 101",
			query: `quantile(0.1, demo_cpu_usage_seconds_total)`,
		},
		{
			name:  "real query 102",
			query: `quantile(0.5, demo_cpu_usage_seconds_total)`,
		},
		{
			name:  "real query 103",
			query: `quantile(0.75, demo_cpu_usage_seconds_total)`,
		},
		{
			name:  "real query 104",
			query: `quantile(0.95, demo_cpu_usage_seconds_total)`,
		},
		{
			name:  "real query 105",
			query: `quantile(0.90, demo_cpu_usage_seconds_total)`,
		},
		{
			name:  "real query 106",
			query: `quantile(0.99, demo_cpu_usage_seconds_total)`,
		},
		{
			name:  "real query 107",
			query: `quantile(1, demo_cpu_usage_seconds_total)`,
		},
		{
			name:  "real query 108",
			query: `quantile(1.5, demo_cpu_usage_seconds_total)`,
		},
		{
			name:  "real query 109",
			query: `avg(max by(mode) (demo_cpu_usage_seconds_total))`,
		},
		{
			name:  "real query 110",
			query: `1 * 2 + 4 / 6 - 10 % 2 ^ 2`,
		},
		{
			name:  "real query 111",
			query: `demo_num_cpus + (1 == bool 2)`,
		},
		{
			name:  "real query 112",
			query: `demo_num_cpus + (1 != bool 2)`,
		},
		{
			name:  "real query 113",
			query: `demo_num_cpus + (1 < bool 2)`,
		},
		{
			name:  "real query 114",
			query: `demo_num_cpus + (1 > bool 2)`,
		},
		{
			name:  "real query 115",
			query: `demo_num_cpus + (1 <= bool 2)`,
		},
		{
			name:  "real query 116",
			query: `demo_num_cpus + (1 >= bool 2)`,
		},
		{
			name:  "real query 117",
			query: `demo_cpu_usage_seconds_total + 1.2345`,
		},
		{
			name:  "real query 118",
			query: `demo_cpu_usage_seconds_total - 1.2345`,
		},
		{
			name:  "real query 119",
			query: `demo_cpu_usage_seconds_total * 1.2345`,
		},
		{
			name:  "real query 120",
			query: `demo_cpu_usage_seconds_total / 1.2345`,
		},
		{
			name:  "real query 121",
			query: `demo_cpu_usage_seconds_total % 1.2345`,
		},
		{
			name:  "real query 122",
			query: `demo_cpu_usage_seconds_total ^ 1.2345`,
		},
		{
			name:  "real query 123",
			query: `demo_cpu_usage_seconds_total == 1.2345`,
		},
		{
			name:  "real query 124",
			query: `demo_cpu_usage_seconds_total != 1.2345`,
		},
		{
			name:  "real query 125",
			query: `demo_cpu_usage_seconds_total < 1.2345`,
		},
		{
			name:  "real query 126",
			query: `demo_cpu_usage_seconds_total > 1.2345`,
		},
		{
			name:  "real query 127",
			query: `demo_cpu_usage_seconds_total <= 1.2345`,
		},
		{
			name:  "real query 128",
			query: `demo_cpu_usage_seconds_total >= 1.2345`,
		},
		{
			name:  "real query 129",
			query: `demo_cpu_usage_seconds_total == bool 1.2345`,
		},
		{
			name:  "real query 130",
			query: `demo_cpu_usage_seconds_total != bool 1.2345`,
		},
		{
			name:  "real query 131",
			query: `demo_cpu_usage_seconds_total < bool 1.2345`,
		},
		{
			name:  "real query 132",
			query: `demo_cpu_usage_seconds_total > bool 1.2345`,
		},
		{
			name:  "real query 133",
			query: `demo_cpu_usage_seconds_total <= bool 1.2345`,
		},
		{
			name:  "real query 134",
			query: `demo_cpu_usage_seconds_total >= bool 1.2345`,
		},
		{
			name:  "real query 135",
			query: `1.2345 == bool demo_cpu_usage_seconds_total`,
		},
		{
			name:  "real query 136",
			query: `1.2345 != bool demo_cpu_usage_seconds_total`,
		},
		{
			name:  "real query 137",
			query: `1.2345 < bool demo_cpu_usage_seconds_total`,
		},
		{
			name:  "real query 138",
			query: `1.2345 > bool demo_cpu_usage_seconds_total`,
		},
		{
			name:  "real query 139",
			query: `1.2345 <= bool demo_cpu_usage_seconds_total`,
		},
		{
			name:  "real query 140",
			query: `1.2345 >= bool demo_cpu_usage_seconds_total`,
		},
		{
			name:  "real query 141",
			query: `0.12345 + demo_cpu_usage_seconds_total`,
		},
		{
			name:  "real query 142",
			query: `0.12345 - demo_cpu_usage_seconds_total`,
		},
		{
			name:  "real query 143",
			query: `0.12345 * demo_cpu_usage_seconds_total`,
		},
		{
			name:  "real query 144",
			query: `0.12345 / demo_cpu_usage_seconds_total`,
		},
		{
			name:  "real query 145",
			query: `0.12345 % demo_cpu_usage_seconds_total`,
		},
		{
			name:  "real query 146",
			query: `0.12345 ^ demo_cpu_usage_seconds_total`,
		},
		{
			name:  "real query 147",
			query: `0.12345 == demo_cpu_usage_seconds_total`,
		},
		{
			name:  "real query 148",
			query: `0.12345 != demo_cpu_usage_seconds_total`,
		},
		{
			name:  "real query 149",
			query: `0.12345 < demo_cpu_usage_seconds_total`,
		},
		{
			name:  "real query 150",
			query: `0.12345 > demo_cpu_usage_seconds_total`,
		},
		{
			name:  "real query 151",
			query: `0.12345 <= demo_cpu_usage_seconds_total`,
		},
		{
			name:  "real query 152",
			query: `0.12345 >= demo_cpu_usage_seconds_total`,
		},
		{
			name:  "real query 153",
			query: `(1 * 2 + 4 / 6 - (10%7)^2) + demo_cpu_usage_seconds_total`,
		},
		{
			name:  "real query 154",
			query: `(1 * 2 + 4 / 6 - (10%7)^2) - demo_cpu_usage_seconds_total`,
		},
		{
			name:  "real query 155",
			query: `(1 * 2 + 4 / 6 - (10%7)^2) * demo_cpu_usage_seconds_total`,
		},
		{
			name:  "real query 156",
			query: `(1 * 2 + 4 / 6 - (10%7)^2) / demo_cpu_usage_seconds_total`,
		},
		{
			name:  "real query 157",
			query: `(1 * 2 + 4 / 6 - (10%7)^2) % demo_cpu_usage_seconds_total`,
		},
		{
			name:  "real query 158",
			query: `(1 * 2 + 4 / 6 - (10%7)^2) ^ demo_cpu_usage_seconds_total`,
		},
		{
			name:  "real query 159",
			query: `(1 * 2 + 4 / 6 - (10%7)^2) == demo_cpu_usage_seconds_total`,
		},
		{
			name:  "real query 160",
			query: `(1 * 2 + 4 / 6 - (10%7)^2) != demo_cpu_usage_seconds_total`,
		},
		{
			name:  "real query 161",
			query: `(1 * 2 + 4 / 6 - (10%7)^2) < demo_cpu_usage_seconds_total`,
		},
		{
			name:  "real query 162",
			query: `(1 * 2 + 4 / 6 - (10%7)^2) > demo_cpu_usage_seconds_total`,
		},
		{
			name:  "real query 163",
			query: `(1 * 2 + 4 / 6 - (10%7)^2) <= demo_cpu_usage_seconds_total`,
		},
		{
			name:  "real query 164",
			query: `(1 * 2 + 4 / 6 - (10%7)^2) >= demo_cpu_usage_seconds_total`,
		},
		{
			name:  "real query 165",
			query: `demo_cpu_usage_seconds_total + (1 * 2 + 4 / 6 - 10)`,
		},
		{
			name:  "real query 166",
			query: `demo_cpu_usage_seconds_total - (1 * 2 + 4 / 6 - 10)`,
		},
		{
			name:  "real query 167",
			query: `demo_cpu_usage_seconds_total * (1 * 2 + 4 / 6 - 10)`,
		},
		{
			name:  "real query 168",
			query: `demo_cpu_usage_seconds_total / (1 * 2 + 4 / 6 - 10)`,
		},
		{
			name:  "real query 169",
			query: `demo_cpu_usage_seconds_total % (1 * 2 + 4 / 6 - 10)`,
		},
		{
			name:  "real query 170",
			query: `demo_cpu_usage_seconds_total ^ (1 * 2 + 4 / 6 - 10)`,
		},
		{
			name:  "real query 171",
			query: `demo_cpu_usage_seconds_total == (1 * 2 + 4 / 6 - 10)`,
		},
		{
			name:  "real query 172",
			query: `demo_cpu_usage_seconds_total != (1 * 2 + 4 / 6 - 10)`,
		},
		{
			name:  "real query 173",
			query: `demo_cpu_usage_seconds_total < (1 * 2 + 4 / 6 - 10)`,
		},
		{
			name:  "real query 174",
			query: `demo_cpu_usage_seconds_total > (1 * 2 + 4 / 6 - 10)`,
		},
		{
			name:  "real query 175",
			query: `demo_cpu_usage_seconds_total <= (1 * 2 + 4 / 6 - 10)`,
		},
		{
			name:  "real query 176",
			query: `demo_cpu_usage_seconds_total >= (1 * 2 + 4 / 6 - 10)`,
		},
		{
			name:  "real query 177",
			query: `timestamp(demo_cpu_usage_seconds_total * 1)`,
		},
		{
			name:  "real query 178",
			query: `timestamp(-demo_cpu_usage_seconds_total)`,
		},
		{
			name:  "real query 179",
			query: `demo_cpu_usage_seconds_total + on(instance, job, mode) demo_cpu_usage_seconds_total`,
		},
		{
			name:  "real query 180",
			query: `demo_cpu_usage_seconds_total - on(instance, job, mode) demo_cpu_usage_seconds_total`,
		},
		{
			name:  "real query 181",
			query: `demo_cpu_usage_seconds_total * on(instance, job, mode) demo_cpu_usage_seconds_total`,
		},
		{
			name:  "real query 182",
			query: `demo_cpu_usage_seconds_total / on(instance, job, mode) demo_cpu_usage_seconds_total`,
		},
		{
			name:  "real query 183",
			query: `demo_cpu_usage_seconds_total % on(instance, job, mode) demo_cpu_usage_seconds_total`,
		},
		{
			name:  "real query 184",
			query: `demo_cpu_usage_seconds_total ^ on(instance, job, mode) demo_cpu_usage_seconds_total`,
		},
		{
			name:  "real query 185",
			query: `demo_cpu_usage_seconds_total == on(instance, job, mode) demo_cpu_usage_seconds_total`,
		},
		{
			name:  "real query 186",
			query: `demo_cpu_usage_seconds_total != on(instance, job, mode) demo_cpu_usage_seconds_total`,
		},
		{
			name:  "real query 187",
			query: `demo_cpu_usage_seconds_total < on(instance, job, mode) demo_cpu_usage_seconds_total`,
		},
		{
			name:  "real query 188",
			query: `demo_cpu_usage_seconds_total > on(instance, job, mode) demo_cpu_usage_seconds_total`,
		},
		{
			name:  "real query 189",
			query: `demo_cpu_usage_seconds_total <= on(instance, job, mode) demo_cpu_usage_seconds_total`,
		},
		{
			name:  "real query 190",
			query: `demo_cpu_usage_seconds_total >= on(instance, job, mode) demo_cpu_usage_seconds_total`,
		},
		{
			name:  "real query 191",
			query: `sum by(instance, mode) (demo_cpu_usage_seconds_total) + on(instance, mode) group_left(job) demo_cpu_usage_seconds_total`,
		},
		{
			name:  "real query 192",
			query: `sum by(instance, mode) (demo_cpu_usage_seconds_total) - on(instance, mode) group_left(job) demo_cpu_usage_seconds_total`,
		},
		{
			name:  "real query 193",
			query: `sum by(instance, mode) (demo_cpu_usage_seconds_total) * on(instance, mode) group_left(job) demo_cpu_usage_seconds_total`,
		},
		{
			name:  "real query 194",
			query: `sum by(instance, mode) (demo_cpu_usage_seconds_total) / on(instance, mode) group_left(job) demo_cpu_usage_seconds_total`,
		},
		{
			name:  "real query 195",
			query: `sum by(instance, mode) (demo_cpu_usage_seconds_total) % on(instance, mode) group_left(job) demo_cpu_usage_seconds_total`,
		},
		{
			name:  "real query 196",
			query: `sum by(instance, mode) (demo_cpu_usage_seconds_total) ^ on(instance, mode) group_left(job) demo_cpu_usage_seconds_total`,
		},
		{
			name:  "real query 197",
			query: `sum by(instance, mode) (demo_cpu_usage_seconds_total) == on(instance, mode) group_left(job) demo_cpu_usage_seconds_total`,
		},
		{
			name:  "real query 198",
			query: `sum by(instance, mode) (demo_cpu_usage_seconds_total) != on(instance, mode) group_left(job) demo_cpu_usage_seconds_total`,
		},
		{
			name:  "real query 199",
			query: `sum by(instance, mode) (demo_cpu_usage_seconds_total) < on(instance, mode) group_left(job) demo_cpu_usage_seconds_total`,
		},
		{
			name:  "real query 200",
			query: `sum by(instance, mode) (demo_cpu_usage_seconds_total) > on(instance, mode) group_left(job) demo_cpu_usage_seconds_total`,
		},
		{
			name:  "real query 201",
			query: `sum by(instance, mode) (demo_cpu_usage_seconds_total) <= on(instance, mode) group_left(job) demo_cpu_usage_seconds_total`,
		},
		{
			name:  "real query 202",
			query: `sum by(instance, mode) (demo_cpu_usage_seconds_total) >= on(instance, mode) group_left(job) demo_cpu_usage_seconds_total`,
		},
		{
			name:  "real query 203",
			query: `demo_cpu_usage_seconds_total == bool on(instance, job, mode) demo_cpu_usage_seconds_total`,
		},
		{
			name:  "real query 204",
			query: `demo_cpu_usage_seconds_total != bool on(instance, job, mode) demo_cpu_usage_seconds_total`,
		},
		{
			name:  "real query 205",
			query: `demo_cpu_usage_seconds_total < bool on(instance, job, mode) demo_cpu_usage_seconds_total`,
		},
		{
			name:  "real query 206",
			query: `demo_cpu_usage_seconds_total > bool on(instance, job, mode) demo_cpu_usage_seconds_total`,
		},
		{
			name:  "real query 207",
			query: `demo_cpu_usage_seconds_total <= bool on(instance, job, mode) demo_cpu_usage_seconds_total`,
		},
		{
			name:  "real query 208",
			query: `demo_cpu_usage_seconds_total >= bool on(instance, job, mode) demo_cpu_usage_seconds_total`,
		},
		{
			name:  "real query 209",
			query: `demo_cpu_usage_seconds_total / on(instance, job, mode, __name__) demo_cpu_usage_seconds_total`,
		},
		{
			name:  "real query 210",
			query: `sum without(job) (demo_cpu_usage_seconds_total) / on(instance, mode) demo_cpu_usage_seconds_total`,
		},
		{
			name:  "real query 211",
			query: `sum without(job) (demo_cpu_usage_seconds_total) / on(instance, mode) group_left demo_cpu_usage_seconds_total`,
		},
		{
			name:  "real query 212",
			query: `sum without(job) (demo_cpu_usage_seconds_total) / on(instance, mode) group_left(job) demo_cpu_usage_seconds_total`,
		},
		{
			name:  "real query 213",
			query: `demo_cpu_usage_seconds_total / on(instance, job) group_left demo_num_cpus`,
		},
		{
			name:  "real query 214",
			query: `demo_cpu_usage_seconds_total / on(instance, mode, job, non_existent) demo_cpu_usage_seconds_total`,
		},
		{
			name:  "real query 215",
			query: `demo_num_cpus * Inf`,
		},
		{
			name:  "real query 216",
			query: `demo_num_cpus * -Inf`,
		},
		{
			name:  "real query 217",
			query: `demo_num_cpus * NaN`,
		},
		{
			name:  "real query 218",
			query: `demo_cpu_usage_seconds_total + -(1)`,
		},
		{
			name:  "real query 219",
			query: `-demo_cpu_usage_seconds_total`,
		},
		{
			name:  "real query 220",
			query: `-1 ^ 2`,
		},
		{
			name:  "real query 221",
			query: `1 + time()`,
		},
		{
			name:  "real query 222",
			query: `1 - time()`,
		},
		{
			name:  "real query 223",
			query: `1 * time()`,
		},
		{
			name:  "real query 224",
			query: `1 / time()`,
		},
		{
			name:  "real query 225",
			query: `1 % time()`,
		},
		{
			name:  "real query 226",
			query: `1 ^ time()`,
		},
		{
			name:  "real query 227",
			query: `time() + 1`,
		},
		{
			name:  "real query 228",
			query: `time() - 1`,
		},
		{
			name:  "real query 229",
			query: `time() * 1`,
		},
		{
			name:  "real query 230",
			query: `time() / 1`,
		},
		{
			name:  "real query 231",
			query: `time() % 1`,
		},
		{
			name:  "real query 232",
			query: `time() ^ 1`,
		},
		{
			name:  "real query 233",
			query: `time() == bool 1`,
		},
		{
			name:  "real query 234",
			query: `time() != bool 1`,
		},
		{
			name:  "real query 235",
			query: `time() < bool 1`,
		},
		{
			name:  "real query 236",
			query: `time() > bool 1`,
		},
		{
			name:  "real query 237",
			query: `time() <= bool 1`,
		},
		{
			name:  "real query 238",
			query: `time() >= bool 1`,
		},
		{
			name:  "real query 239",
			query: `1 == bool time()`,
		},
		{
			name:  "real query 240",
			query: `1 != bool time()`,
		},
		{
			name:  "real query 241",
			query: `1 < bool time()`,
		},
		{
			name:  "real query 242",
			query: `1 > bool time()`,
		},
		{
			name:  "real query 243",
			query: `1 <= bool time()`,
		},
		{
			name:  "real query 244",
			query: `1 >= bool time()`,
		},
		{
			name:  "real query 245",
			query: `time() + time()`,
		},
		{
			name:  "real query 246",
			query: `time() - time()`,
		},
		{
			name:  "real query 247",
			query: `time() * time()`,
		},
		{
			name:  "real query 248",
			query: `time() / time()`,
		},
		{
			name:  "real query 249",
			query: `time() % time()`,
		},
		{
			name:  "real query 250",
			query: `time() ^ time()`,
		},
		{
			name:  "real query 251",
			query: `time() == bool time()`,
		},
		{
			name:  "real query 252",
			query: `time() != bool time()`,
		},
		{
			name:  "real query 253",
			query: `time() < bool time()`,
		},
		{
			name:  "real query 254",
			query: `time() > bool time()`,
		},
		{
			name:  "real query 255",
			query: `time() <= bool time()`,
		},
		{
			name:  "real query 256",
			query: `time() >= bool time()`,
		},
		{
			name:  "real query 257",
			query: `time() + demo_cpu_usage_seconds_total`,
		},
		{
			name:  "real query 258",
			query: `time() - demo_cpu_usage_seconds_total`,
		},
		{
			name:  "real query 259",
			query: `time() * demo_cpu_usage_seconds_total`,
		},
		{
			name:  "real query 260",
			query: `time() / demo_cpu_usage_seconds_total`,
		},
		{
			name:  "real query 261",
			query: `time() % demo_cpu_usage_seconds_total`,
		},
		{
			name:  "real query 262",
			query: `time() ^ demo_cpu_usage_seconds_total`,
		},
		{
			name:  "real query 263",
			query: `time() == demo_cpu_usage_seconds_total`,
		},
		{
			name:  "real query 264",
			query: `time() != demo_cpu_usage_seconds_total`,
		},
		{
			name:  "real query 265",
			query: `time() < demo_cpu_usage_seconds_total`,
		},
		{
			name:  "real query 266",
			query: `time() > demo_cpu_usage_seconds_total`,
		},
		{
			name:  "real query 267",
			query: `time() <= demo_cpu_usage_seconds_total`,
		},
		{
			name:  "real query 268",
			query: `time() >= demo_cpu_usage_seconds_total`,
		},
		{
			name:  "real query 269",
			query: `demo_cpu_usage_seconds_total + time()`,
		},
		{
			name:  "real query 270",
			query: `demo_cpu_usage_seconds_total - time()`,
		},
		{
			name:  "real query 271",
			query: `demo_cpu_usage_seconds_total * time()`,
		},
		{
			name:  "real query 272",
			query: `demo_cpu_usage_seconds_total / time()`,
		},
		{
			name:  "real query 273",
			query: `demo_cpu_usage_seconds_total % time()`,
		},
		{
			name:  "real query 274",
			query: `demo_cpu_usage_seconds_total ^ time()`,
		},
		{
			name:  "real query 275",
			query: `demo_cpu_usage_seconds_total == time()`,
		},
		{
			name:  "real query 276",
			query: `demo_cpu_usage_seconds_total != time()`,
		},
		{
			name:  "real query 277",
			query: `demo_cpu_usage_seconds_total < time()`,
		},
		{
			name:  "real query 278",
			query: `demo_cpu_usage_seconds_total > time()`,
		},
		{
			name:  "real query 279",
			query: `demo_cpu_usage_seconds_total <= time()`,
		},
		{
			name:  "real query 280",
			query: `demo_cpu_usage_seconds_total >= time()`,
		},
		{
			name:  "real query 281",
			query: `sum_over_time(demo_cpu_usage_seconds_total[1s])`,
		},
		{
			name:  "real query 282",
			query: `sum_over_time(demo_cpu_usage_seconds_total[15s])`,
		},
		{
			name:  "real query 283",
			query: `sum_over_time(demo_cpu_usage_seconds_total[1m])`,
		},
		{
			name:  "real query 284",
			query: `sum_over_time(demo_cpu_usage_seconds_total[5m])`,
		},
		{
			name:  "real query 285",
			query: `sum_over_time(demo_cpu_usage_seconds_total[15m])`,
		},
		{
			name:  "real query 286",
			query: `sum_over_time(demo_cpu_usage_seconds_total[1h])`,
		},
		{
			name:  "real query 287",
			query: `avg_over_time(demo_cpu_usage_seconds_total[1s])`,
		},
		{
			name:  "real query 288",
			query: `avg_over_time(demo_cpu_usage_seconds_total[15s])`,
		},
		{
			name:  "real query 289",
			query: `avg_over_time(demo_cpu_usage_seconds_total[1m])`,
		},
		{
			name:  "real query 290",
			query: `avg_over_time(demo_cpu_usage_seconds_total[5m])`,
		},
		{
			name:  "real query 291",
			query: `avg_over_time(demo_cpu_usage_seconds_total[15m])`,
		},
		{
			name:  "real query 292",
			query: `avg_over_time(demo_cpu_usage_seconds_total[1h])`,
		},
		{
			name:  "real query 293",
			query: `max_over_time(demo_cpu_usage_seconds_total[1s])`,
		},
		{
			name:  "real query 294",
			query: `max_over_time(demo_cpu_usage_seconds_total[15s])`,
		},
		{
			name:  "real query 295",
			query: `max_over_time(demo_cpu_usage_seconds_total[1m])`,
		},
		{
			name:  "real query 296",
			query: `max_over_time(demo_cpu_usage_seconds_total[5m])`,
		},
		{
			name:  "real query 297",
			query: `max_over_time(demo_cpu_usage_seconds_total[15m])`,
		},
		{
			name:  "real query 298",
			query: `max_over_time(demo_cpu_usage_seconds_total[1h])`,
		},
		{
			name:  "real query 299",
			query: `min_over_time(demo_cpu_usage_seconds_total[1s])`,
		},
		{
			name:  "real query 300",
			query: `min_over_time(demo_cpu_usage_seconds_total[15s])`,
		},
		{
			name:  "real query 301",
			query: `min_over_time(demo_cpu_usage_seconds_total[1m])`,
		},
		{
			name:  "real query 302",
			query: `min_over_time(demo_cpu_usage_seconds_total[5m])`,
		},
		{
			name:  "real query 303",
			query: `min_over_time(demo_cpu_usage_seconds_total[15m])`,
		},
		{
			name:  "real query 304",
			query: `min_over_time(demo_cpu_usage_seconds_total[1h])`,
		},
		{
			name:  "real query 305",
			query: `count_over_time(demo_cpu_usage_seconds_total[1s])`,
		},
		{
			name:  "real query 306",
			query: `count_over_time(demo_cpu_usage_seconds_total[15s])`,
		},
		{
			name:  "real query 307",
			query: `count_over_time(demo_cpu_usage_seconds_total[1m])`,
		},
		{
			name:  "real query 308",
			query: `count_over_time(demo_cpu_usage_seconds_total[5m])`,
		},
		{
			name:  "real query 309",
			query: `count_over_time(demo_cpu_usage_seconds_total[15m])`,
		},
		{
			name:  "real query 310",
			query: `count_over_time(demo_cpu_usage_seconds_total[1h])`,
		},
		{
			name:  "real query 311",
			query: `stddev_over_time(demo_cpu_usage_seconds_total[1s])`,
		},
		{
			name:  "real query 312",
			query: `stddev_over_time(demo_cpu_usage_seconds_total[15s])`,
		},
		{
			name:  "real query 313",
			query: `stddev_over_time(demo_cpu_usage_seconds_total[1m])`,
		},
		{
			name:  "real query 314",
			query: `stddev_over_time(demo_cpu_usage_seconds_total[5m])`,
		},
		{
			name:  "real query 315",
			query: `stddev_over_time(demo_cpu_usage_seconds_total[15m])`,
		},
		{
			name:  "real query 316",
			query: `stddev_over_time(demo_cpu_usage_seconds_total[1h])`,
		},
		{
			name:  "real query 317",
			query: `stdvar_over_time(demo_cpu_usage_seconds_total[1s])`,
		},
		{
			name:  "real query 318",
			query: `stdvar_over_time(demo_cpu_usage_seconds_total[15s])`,
		},
		{
			name:  "real query 319",
			query: `stdvar_over_time(demo_cpu_usage_seconds_total[1m])`,
		},
		{
			name:  "real query 320",
			query: `stdvar_over_time(demo_cpu_usage_seconds_total[5m])`,
		},
		{
			name:  "real query 321",
			query: `stdvar_over_time(demo_cpu_usage_seconds_total[15m])`,
		},
		{
			name:  "real query 322",
			query: `stdvar_over_time(demo_cpu_usage_seconds_total[1h])`,
		},
		{
			name:  "real query 323",
			query: `quantile_over_time(-0.5, demo_cpu_usage_seconds_total[1s])`,
		},
		{
			name:  "real query 324",
			query: `quantile_over_time(-0.5, demo_cpu_usage_seconds_total[15s])`,
		},
		{
			name:  "real query 325",
			query: `quantile_over_time(-0.5, demo_cpu_usage_seconds_total[1m])`,
		},
		{
			name:  "real query 326",
			query: `quantile_over_time(-0.5, demo_cpu_usage_seconds_total[5m])`,
		},
		{
			name:  "real query 327",
			query: `quantile_over_time(-0.5, demo_cpu_usage_seconds_total[15m])`,
		},
		{
			name:  "real query 328",
			query: `quantile_over_time(-0.5, demo_cpu_usage_seconds_total[1h])`,
		},
		{
			name:  "real query 329",
			query: `quantile_over_time(0.1, demo_cpu_usage_seconds_total[1s])`,
		},
		{
			name:  "real query 330",
			query: `quantile_over_time(0.1, demo_cpu_usage_seconds_total[15s])`,
		},
		{
			name:  "real query 331",
			query: `quantile_over_time(0.1, demo_cpu_usage_seconds_total[1m])`,
		},
		{
			name:  "real query 332",
			query: `quantile_over_time(0.1, demo_cpu_usage_seconds_total[5m])`,
		},
		{
			name:  "real query 333",
			query: `quantile_over_time(0.1, demo_cpu_usage_seconds_total[15m])`,
		},
		{
			name:  "real query 334",
			query: `quantile_over_time(0.1, demo_cpu_usage_seconds_total[1h])`,
		},
		{
			name:  "real query 335",
			query: `quantile_over_time(0.5, demo_cpu_usage_seconds_total[1s])`,
		},
		{
			name:  "real query 336",
			query: `quantile_over_time(0.5, demo_cpu_usage_seconds_total[15s])`,
		},
		{
			name:  "real query 337",
			query: `quantile_over_time(0.5, demo_cpu_usage_seconds_total[1m])`,
		},
		{
			name:  "real query 338",
			query: `quantile_over_time(0.5, demo_cpu_usage_seconds_total[5m])`,
		},
		{
			name:  "real query 339",
			query: `quantile_over_time(0.5, demo_cpu_usage_seconds_total[15m])`,
		},
		{
			name:  "real query 340",
			query: `quantile_over_time(0.5, demo_cpu_usage_seconds_total[1h])`,
		},
		{
			name:  "real query 341",
			query: `quantile_over_time(0.75, demo_cpu_usage_seconds_total[1s])`,
		},
		{
			name:  "real query 342",
			query: `quantile_over_time(0.75, demo_cpu_usage_seconds_total[15s])`,
		},
		{
			name:  "real query 343",
			query: `quantile_over_time(0.75, demo_cpu_usage_seconds_total[1m])`,
		},
		{
			name:  "real query 344",
			query: `quantile_over_time(0.75, demo_cpu_usage_seconds_total[5m])`,
		},
		{
			name:  "real query 345",
			query: `quantile_over_time(0.75, demo_cpu_usage_seconds_total[15m])`,
		},
		{
			name:  "real query 346",
			query: `quantile_over_time(0.75, demo_cpu_usage_seconds_total[1h])`,
		},
		{
			name:  "real query 347",
			query: `quantile_over_time(0.95, demo_cpu_usage_seconds_total[1s])`,
		},
		{
			name:  "real query 348",
			query: `quantile_over_time(0.95, demo_cpu_usage_seconds_total[15s])`,
		},
		{
			name:  "real query 349",
			query: `quantile_over_time(0.95, demo_cpu_usage_seconds_total[1m])`,
		},
		{
			name:  "real query 350",
			query: `quantile_over_time(0.95, demo_cpu_usage_seconds_total[5m])`,
		},
		{
			name:  "real query 351",
			query: `quantile_over_time(0.95, demo_cpu_usage_seconds_total[15m])`,
		},
		{
			name:  "real query 352",
			query: `quantile_over_time(0.95, demo_cpu_usage_seconds_total[1h])`,
		},
		{
			name:  "real query 353",
			query: `quantile_over_time(0.90, demo_cpu_usage_seconds_total[1s])`,
		},
		{
			name:  "real query 354",
			query: `quantile_over_time(0.90, demo_cpu_usage_seconds_total[15s])`,
		},
		{
			name:  "real query 355",
			query: `quantile_over_time(0.90, demo_cpu_usage_seconds_total[1m])`,
		},
		{
			name:  "real query 356",
			query: `quantile_over_time(0.90, demo_cpu_usage_seconds_total[5m])`,
		},
		{
			name:  "real query 357",
			query: `quantile_over_time(0.90, demo_cpu_usage_seconds_total[15m])`,
		},
		{
			name:  "real query 358",
			query: `quantile_over_time(0.90, demo_cpu_usage_seconds_total[1h])`,
		},
		{
			name:  "real query 359",
			query: `quantile_over_time(0.99, demo_cpu_usage_seconds_total[1s])`,
		},
		{
			name:  "real query 360",
			query: `quantile_over_time(0.99, demo_cpu_usage_seconds_total[15s])`,
		},
		{
			name:  "real query 361",
			query: `quantile_over_time(0.99, demo_cpu_usage_seconds_total[1m])`,
		},
		{
			name:  "real query 362",
			query: `quantile_over_time(0.99, demo_cpu_usage_seconds_total[5m])`,
		},
		{
			name:  "real query 363",
			query: `quantile_over_time(0.99, demo_cpu_usage_seconds_total[15m])`,
		},
		{
			name:  "real query 364",
			query: `quantile_over_time(0.99, demo_cpu_usage_seconds_total[1h])`,
		},
		{
			name:  "real query 365",
			query: `quantile_over_time(1, demo_cpu_usage_seconds_total[1s])`,
		},
		{
			name:  "real query 366",
			query: `quantile_over_time(1, demo_cpu_usage_seconds_total[15s])`,
		},
		{
			name:  "real query 367",
			query: `quantile_over_time(1, demo_cpu_usage_seconds_total[1m])`,
		},
		{
			name:  "real query 368",
			query: `quantile_over_time(1, demo_cpu_usage_seconds_total[5m])`,
		},
		{
			name:  "real query 369",
			query: `quantile_over_time(1, demo_cpu_usage_seconds_total[15m])`,
		},
		{
			name:  "real query 370",
			query: `quantile_over_time(1, demo_cpu_usage_seconds_total[1h])`,
		},
		{
			name:  "real query 371",
			query: `quantile_over_time(1.5, demo_cpu_usage_seconds_total[1s])`,
		},
		{
			name:  "real query 372",
			query: `quantile_over_time(1.5, demo_cpu_usage_seconds_total[15s])`,
		},
		{
			name:  "real query 373",
			query: `quantile_over_time(1.5, demo_cpu_usage_seconds_total[1m])`,
		},
		{
			name:  "real query 374",
			query: `quantile_over_time(1.5, demo_cpu_usage_seconds_total[5m])`,
		},
		{
			name:  "real query 375",
			query: `quantile_over_time(1.5, demo_cpu_usage_seconds_total[15m])`,
		},
		{
			name:  "real query 376",
			query: `quantile_over_time(1.5, demo_cpu_usage_seconds_total[1h])`,
		},
		{
			name:  "real query 377",
			query: `timestamp(demo_num_cpus)`,
		},
		{
			name:  "real query 378",
			query: `timestamp(timestamp(demo_num_cpus))`,
		},
		{
			name:  "real query 379",
			query: `abs(demo_cpu_usage_seconds_total)`,
		},
		{
			name:  "real query 380",
			query: `ceil(demo_cpu_usage_seconds_total)`,
		},
		{
			name:  "real query 381",
			query: `floor(demo_cpu_usage_seconds_total)`,
		},
		{
			name:  "real query 382",
			query: `exp(demo_cpu_usage_seconds_total)`,
		},
		{
			name:  "real query 383",
			query: `sqrt(demo_cpu_usage_seconds_total)`,
		},
		{
			name:  "real query 384",
			query: `ln(demo_cpu_usage_seconds_total)`,
		},
		{
			name:  "real query 385",
			query: `log2(demo_cpu_usage_seconds_total)`,
		},
		{
			name:  "real query 386",
			query: `log10(demo_cpu_usage_seconds_total)`,
		},
		{
			name:  "real query 387",
			query: `round(demo_cpu_usage_seconds_total)`,
		},
		{
			name:  "real query 388",
			query: `abs(-demo_cpu_usage_seconds_total)`,
		},
		{
			name:  "real query 389",
			query: `ceil(-demo_cpu_usage_seconds_total)`,
		},
		{
			name:  "real query 390",
			query: `floor(-demo_cpu_usage_seconds_total)`,
		},
		{
			name:  "real query 391",
			query: `exp(-demo_cpu_usage_seconds_total)`,
		},
		{
			name:  "real query 392",
			query: `sqrt(-demo_cpu_usage_seconds_total)`,
		},
		{
			name:  "real query 393",
			query: `ln(-demo_cpu_usage_seconds_total)`,
		},
		{
			name:  "real query 394",
			query: `log2(-demo_cpu_usage_seconds_total)`,
		},
		{
			name:  "real query 395",
			query: `log10(-demo_cpu_usage_seconds_total)`,
		},
		{
			name:  "real query 396",
			query: `round(-demo_cpu_usage_seconds_total)`,
		},
		{
			name:  "real query 397",
			query: `delta(nonexistent_metric[5m])`,
		},
		{
			name:  "real query 398",
			query: `rate(nonexistent_metric[5m])`,
		},
		{
			name:  "real query 399",
			query: `increase(nonexistent_metric[5m])`,
		},
		{
			name:  "real query 400",
			query: `delta(demo_cpu_usage_seconds_total[1s])`,
		},
		{
			name:  "real query 401",
			query: `delta(demo_cpu_usage_seconds_total[15s])`,
		},
		{
			name:  "real query 402",
			query: `delta(demo_cpu_usage_seconds_total[1m])`,
		},
		{
			name:  "real query 403",
			query: `delta(demo_cpu_usage_seconds_total[5m])`,
		},
		{
			name:  "real query 404",
			query: `delta(demo_cpu_usage_seconds_total[15m])`,
		},
		{
			name:  "real query 405",
			query: `delta(demo_cpu_usage_seconds_total[1h])`,
		},
		{
			name:  "real query 406",
			query: `rate(demo_cpu_usage_seconds_total[1s])`,
		},
		{
			name:  "real query 407",
			query: `rate(demo_cpu_usage_seconds_total[15s])`,
		},
		{
			name:  "real query 408",
			query: `rate(demo_cpu_usage_seconds_total[1m])`,
		},
		{
			name:  "real query 409",
			query: `rate(demo_cpu_usage_seconds_total[5m])`,
		},
		{
			name:  "real query 410",
			query: `rate(demo_cpu_usage_seconds_total[15m])`,
		},
		{
			name:  "real query 411",
			query: `rate(demo_cpu_usage_seconds_total[1h])`,
		},
		{
			name:  "real query 412",
			query: `increase(demo_cpu_usage_seconds_total[1s])`,
		},
		{
			name:  "real query 413",
			query: `increase(demo_cpu_usage_seconds_total[15s])`,
		},
		{
			name:  "real query 414",
			query: `increase(demo_cpu_usage_seconds_total[1m])`,
		},
		{
			name:  "real query 415",
			query: `increase(demo_cpu_usage_seconds_total[5m])`,
		},
		{
			name:  "real query 416",
			query: `increase(demo_cpu_usage_seconds_total[15m])`,
		},
		{
			name:  "real query 417",
			query: `increase(demo_cpu_usage_seconds_total[1h])`,
		},
		{
			name:  "real query 418",
			query: `deriv(demo_disk_usage_bytes[1s])`,
		},
		{
			name:  "real query 419",
			query: `deriv(demo_disk_usage_bytes[15s])`,
		},
		{
			name:  "real query 420",
			query: `deriv(demo_disk_usage_bytes[1m])`,
		},
		{
			name:  "real query 421",
			query: `deriv(demo_disk_usage_bytes[5m])`,
		},
		{
			name:  "real query 422",
			query: `deriv(demo_disk_usage_bytes[15m])`,
		},
		{
			name:  "real query 423",
			query: `deriv(demo_disk_usage_bytes[1h])`,
		},
		{
			name:  "real query 424",
			query: `predict_linear(demo_disk_usage_bytes[1s], 600)`,
		},
		{
			name:  "real query 425",
			query: `predict_linear(demo_disk_usage_bytes[15s], 600)`,
		},
		{
			name:  "real query 426",
			query: `predict_linear(demo_disk_usage_bytes[1m], 600)`,
		},
		{
			name:  "real query 427",
			query: `predict_linear(demo_disk_usage_bytes[5m], 600)`,
		},
		{
			name:  "real query 428",
			query: `predict_linear(demo_disk_usage_bytes[15m], 600)`,
		},
		{
			name:  "real query 429",
			query: `predict_linear(demo_disk_usage_bytes[1h], 600)`,
		},
		{
			name:  "real query 430",
			query: `time()`,
		},
		{
			name:  "real query 431",
			query: `label_replace(demo_num_cpus, "job", "destination-value-$1", "instance", "demo.promlabs.com:(.*)")`,
		},
		{
			name:  "real query 432",
			query: `label_replace(demo_num_cpus, "job", "destination-value-$1", "instance", "host:(.*)")`,
		},
		{
			name:  "real query 433",
			query: `label_replace(demo_num_cpus, "job", "$1-$2", "instance", "local(.*):(.*)")`,
		},
		{
			name:  "real query 434",
			query: `label_replace(demo_num_cpus, "job", "value-$1", "nonexistent-src", "source-value-(.*)")`,
		},
		{
			name:  "real query 435",
			query: `label_replace(demo_num_cpus, "job", "value-$1", "nonexistent-src", "(.*)")`,
		},
		{
			name:  "real query 436",
			query: `label_replace(demo_num_cpus, "job", "value-$1", "instance", "non-matching-regex")`,
		},
		{
			name:  "real query 437",
			query: `label_replace(demo_num_cpus, "job", "", "dst", ".*")`,
		},
		{
			name:  "real query 438",
			query: `label_replace(demo_num_cpus, "job", "value-$1", "src", "(.*")`,
		},
		{
			name:  "real query 439",
			query: `label_replace(demo_num_cpus, "~invalid", "", "src", "(.*)")`,
		},
		{
			name:  "real query 440",
			query: `label_replace(demo_num_cpus, "instance", "", "", "")`,
		},
		{
			name:  "real query 441",
			query: `label_join(demo_num_cpus, "new_label", "-", "instance", "job")`,
		},
		{
			name:  "real query 442",
			query: `label_join(demo_num_cpus, "job", "-", "instance", "job")`,
		},
		{
			name:  "real query 443",
			query: `label_join(demo_num_cpus, "job", "-", "instance")`,
		},
		{
			name:  "real query 444",
			query: `label_join(demo_num_cpus, "~invalid", "-", "instance")`,
		},
		{
			name:  "real query 445",
			query: `day_of_month()`,
		},
		{
			name:  "real query 446",
			query: `day_of_week()`,
		},
		{
			name:  "real query 447",
			query: `days_in_month()`,
		},
		{
			name:  "real query 448",
			query: `hour()`,
		},
		{
			name:  "real query 449",
			query: `minute()`,
		},
		{
			name:  "real query 450",
			query: `month()`,
		},
		{
			name:  "real query 451",
			query: `year()`,
		},
		{
			name:  "real query 452",
			query: `day_of_month(demo_batch_last_success_timestamp_seconds offset 1m)`,
		},
		{
			name:  "real query 453",
			query: `day_of_month(demo_batch_last_success_timestamp_seconds offset 5m)`,
		},
		{
			name:  "real query 454",
			query: `day_of_month(demo_batch_last_success_timestamp_seconds offset 10m)`,
		},
		{
			name:  "real query 455",
			query: `day_of_week(demo_batch_last_success_timestamp_seconds offset 1m)`,
		},
		{
			name:  "real query 456",
			query: `day_of_week(demo_batch_last_success_timestamp_seconds offset 5m)`,
		},
		{
			name:  "real query 457",
			query: `day_of_week(demo_batch_last_success_timestamp_seconds offset 10m)`,
		},
		{
			name:  "real query 458",
			query: `days_in_month(demo_batch_last_success_timestamp_seconds offset 1m)`,
		},
		{
			name:  "real query 459",
			query: `days_in_month(demo_batch_last_success_timestamp_seconds offset 5m)`,
		},
		{
			name:  "real query 460",
			query: `days_in_month(demo_batch_last_success_timestamp_seconds offset 10m)`,
		},
		{
			name:  "real query 461",
			query: `hour(demo_batch_last_success_timestamp_seconds offset 1m)`,
		},
		{
			name:  "real query 462",
			query: `hour(demo_batch_last_success_timestamp_seconds offset 5m)`,
		},
		{
			name:  "real query 463",
			query: `hour(demo_batch_last_success_timestamp_seconds offset 10m)`,
		},
		{
			name:  "real query 464",
			query: `minute(demo_batch_last_success_timestamp_seconds offset 1m)`,
		},
		{
			name:  "real query 465",
			query: `minute(demo_batch_last_success_timestamp_seconds offset 5m)`,
		},
		{
			name:  "real query 466",
			query: `minute(demo_batch_last_success_timestamp_seconds offset 10m)`,
		},
		{
			name:  "real query 467",
			query: `month(demo_batch_last_success_timestamp_seconds offset 1m)`,
		},
		{
			name:  "real query 468",
			query: `month(demo_batch_last_success_timestamp_seconds offset 5m)`,
		},
		{
			name:  "real query 469",
			query: `month(demo_batch_last_success_timestamp_seconds offset 10m)`,
		},
		{
			name:  "real query 470",
			query: `year(demo_batch_last_success_timestamp_seconds offset 1m)`,
		},
		{
			name:  "real query 471",
			query: `year(demo_batch_last_success_timestamp_seconds offset 5m)`,
		},
		{
			name:  "real query 472",
			query: `year(demo_batch_last_success_timestamp_seconds offset 10m)`,
		},
		{
			name:  "real query 473",
			query: `idelta(demo_cpu_usage_seconds_total[1s])`,
		},
		{
			name:  "real query 474",
			query: `idelta(demo_cpu_usage_seconds_total[15s])`,
		},
		{
			name:  "real query 475",
			query: `idelta(demo_cpu_usage_seconds_total[1m])`,
		},
		{
			name:  "real query 476",
			query: `idelta(demo_cpu_usage_seconds_total[5m])`,
		},
		{
			name:  "real query 477",
			query: `idelta(demo_cpu_usage_seconds_total[15m])`,
		},
		{
			name:  "real query 478",
			query: `idelta(demo_cpu_usage_seconds_total[1h])`,
		},
		{
			name:  "real query 479",
			query: `irate(demo_cpu_usage_seconds_total[1s])`,
		},
		{
			name:  "real query 480",
			query: `irate(demo_cpu_usage_seconds_total[15s])`,
		},
		{
			name:  "real query 481",
			query: `irate(demo_cpu_usage_seconds_total[1m])`,
		},
		{
			name:  "real query 482",
			query: `irate(demo_cpu_usage_seconds_total[5m])`,
		},
		{
			name:  "real query 483",
			query: `irate(demo_cpu_usage_seconds_total[15m])`,
		},
		{
			name:  "real query 484",
			query: `irate(demo_cpu_usage_seconds_total[1h])`,
		},
		{
			name:  "real query 485",
			query: `clamp_min(demo_cpu_usage_seconds_total, 2)`,
		},
		{
			name:  "real query 486",
			query: `clamp_max(demo_cpu_usage_seconds_total, 2)`,
		},
		{
			name:  "real query 487",
			query: `resets(demo_cpu_usage_seconds_total[1s])`,
		},
		{
			name:  "real query 488",
			query: `resets(demo_cpu_usage_seconds_total[15s])`,
		},
		{
			name:  "real query 489",
			query: `resets(demo_cpu_usage_seconds_total[1m])`,
		},
		{
			name:  "real query 490",
			query: `resets(demo_cpu_usage_seconds_total[5m])`,
		},
		{
			name:  "real query 491",
			query: `resets(demo_cpu_usage_seconds_total[15m])`,
		},
		{
			name:  "real query 492",
			query: `resets(demo_cpu_usage_seconds_total[1h])`,
		},
		{
			name:  "real query 493",
			query: `changes(demo_batch_last_success_timestamp_seconds[1s])`,
		},
		{
			name:  "real query 494",
			query: `changes(demo_batch_last_success_timestamp_seconds[15s])`,
		},
		{
			name:  "real query 495",
			query: `changes(demo_batch_last_success_timestamp_seconds[1m])`,
		},
		{
			name:  "real query 496",
			query: `changes(demo_batch_last_success_timestamp_seconds[5m])`,
		},
		{
			name:  "real query 497",
			query: `changes(demo_batch_last_success_timestamp_seconds[15m])`,
		},
		{
			name:  "real query 498",
			query: `changes(demo_batch_last_success_timestamp_seconds[1h])`,
		},
		{
			name:  "real query 499",
			query: `vector(1.23)`,
		},
		{
			name:  "real query 500",
			query: `vector(time())`,
		},
		{
			name:  "real query 501",
			query: `histogram_quantile(-0.5, rate(demo_api_request_duration_seconds_bucket[1m]))`,
		},
		{
			name:  "real query 502",
			query: `histogram_quantile(0.1, rate(demo_api_request_duration_seconds_bucket[1m]))`,
		},
		{
			name:  "real query 503",
			query: `histogram_quantile(0.5, rate(demo_api_request_duration_seconds_bucket[1m]))`,
		},
		{
			name:  "real query 504",
			query: `histogram_quantile(0.75, rate(demo_api_request_duration_seconds_bucket[1m]))`,
		},
		{
			name:  "real query 505",
			query: `histogram_quantile(0.95, rate(demo_api_request_duration_seconds_bucket[1m]))`,
		},
		{
			name:  "real query 506",
			query: `histogram_quantile(0.90, rate(demo_api_request_duration_seconds_bucket[1m]))`,
		},
		{
			name:  "real query 507",
			query: `histogram_quantile(0.99, rate(demo_api_request_duration_seconds_bucket[1m]))`,
		},
		{
			name:  "real query 508",
			query: `histogram_quantile(1, rate(demo_api_request_duration_seconds_bucket[1m]))`,
		},
		{
			name:  "real query 509",
			query: `histogram_quantile(1.5, rate(demo_api_request_duration_seconds_bucket[1m]))`,
		},
		{
			name:  "real query 510",
			query: `histogram_quantile(0.9, nonexistent_metric)`,
		},
		{
			name:  "real query 511",
			query: `histogram_quantile(0.9, demo_cpu_usage_seconds_total)`,
		},
		{
			name:  "real query 512",
			query: `histogram_quantile(0.9, {__name__=~"demo_api_request_duration_seconds_.+"})`,
		},
		{
			name:  "real query 513",
			query: `holt_winters(demo_disk_usage_bytes[10m], 0.1, 0.1)`,
		},
		{
			name:  "real query 514",
			query: `holt_winters(demo_disk_usage_bytes[10m], 0.1, 0.5)`,
		},
		{
			name:  "real query 515",
			query: `holt_winters(demo_disk_usage_bytes[10m], 0.1, 0.8)`,
		},
		{
			name:  "real query 516",
			query: `holt_winters(demo_disk_usage_bytes[10m], 0.5, 0.1)`,
		},
		{
			name:  "real query 517",
			query: `holt_winters(demo_disk_usage_bytes[10m], 0.5, 0.5)`,
		},
		{
			name:  "real query 518",
			query: `holt_winters(demo_disk_usage_bytes[10m], 0.5, 0.8)`,
		},
		{
			name:  "real query 519",
			query: `holt_winters(demo_disk_usage_bytes[10m], 0.8, 0.1)`,
		},
		{
			name:  "real query 520",
			query: `holt_winters(demo_disk_usage_bytes[10m], 0.8, 0.5)`,
		},
		{
			name:  "real query 521",
			query: `holt_winters(demo_disk_usage_bytes[10m], 0.8, 0.8)`,
		},
		{
			name:  "real query 522",
			query: `max_over_time((time() - max(demo_batch_last_success_timestamp_seconds) < 1000)[5m:10s] offset 5m)`,
		},
		{
			name:  "real query 523",
			query: `avg_over_time(rate(demo_cpu_usage_seconds_total[1m])[2m:10s])`,
		},
	}
	start := time.Unix(samplesStartTime/1000, 0)
	end := time.Unix(samplesEndTime/1000, 0)
	runPromQLQueryTests(t, testCases, start, end)
}

func TestPromQLQueryEndpoint(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}
	start := time.Unix(startTime/1000, 0)
	end := time.Unix(endTime/1000, 0)

	testCases := []testCase{
		{
			name:  "basic query",
			query: "metric_1",
		},
		{
			name:  "basic query, not regex match metric name",
			query: `{__name__!~".*_1", instance="1"}`,
		},
		{
			name:  "basic query, regex match metric name",
			query: `{__name__=~"metric_.*"}`,
		},
		{
			name:  "basic query, regex no wildcards",
			query: `{__name__=~"metric_1"}`,
		},
		{
			name:  "basic query, no metric name matchers",
			query: `{instance="1", foo=""}`,
		},
		{
			name:  "basic query, multiple matchers",
			query: `{__name__!="metric_1", instance="1"}`,
		},
		{
			name:  "basic query, non-existant metric",
			query: `nonexistant_metric_name`,
		},
		{
			name:  "basic query, with offset",
			query: `metric_3 offset 5m`,
		},
		{
			name:  "basic aggregator",
			query: `sum (metric_3)`,
		},
		{
			name:  "aggregator by empty",
			query: `avg by() (metric_2)`,
		},
		{
			name:  "aggregator by instance",
			query: `max by(instance) (metric_1)`,
		},
		{
			name:  "aggregator by instance and foo",
			query: `min by(instance, foo) (metric_3)`,
		},
		{
			name:  "aggregator by non-existant",
			query: `count by(nonexistant) (metric_2)`,
		},
		{
			name:  "aggregator without empty",
			query: `avg without() (metric_2)`,
		},
		{
			name:  "aggregator without instance",
			query: `max without(instance) (metric_1)`,
		},
		{
			name:  "aggregator without instance and foo",
			query: `min without(instance, foo) (metric_3)`,
		},
		{
			name:  "aggregator without non-existant",
			query: `count without(nonexistant) (metric_2)`,
		},
		{
			name:  "topk",
			query: `topk (3, metric_2)`,
		},
		{
			name:  "bottomk",
			query: `bottomk (3, metric_1)`,
		},
		{
			name:  "topk by instance",
			query: `topk by(instance) (2, metric_3)`,
		},
		{
			name:  "quantile 0.5",
			query: `quantile(0.5, metric_1)`,
		},
		{
			name:  "quantile 0.1",
			query: `quantile(0.1, metric_2)`,
		},
		{
			name:  "quantile 0.95",
			query: `quantile(0.95, metric_3)`,
		},
		{
			name:  "sum_over_time",
			query: `sum_over_time(metric_1[5m])`,
		},
		{
			name:  "count_over_time",
			query: `count_over_time(metric_2[5m])`,
		},
		{
			name:  "avg_over_time",
			query: `avg_over_time(metric_3[5m])`,
		},
		{
			name:  "min_over_time",
			query: `min_over_time(metric_1[5m])`,
		},
		{
			name:  "max_over_time",
			query: `max_over_time(metric_2[5m])`,
		},
		{
			name:  "stddev_over_time",
			query: `stddev_over_time(metric_2[5m])`,
		},
		{
			name:  "delta",
			query: `delta(metric_3[5m])`,
		},
		{
			name:  "delta 1m",
			query: `delta(metric_3[1m])`,
		},
		{
			name:  "increase",
			query: `increase(metric_1[5m])`,
		},
		{
			name:  "rate",
			query: `rate(metric_2[5m])`,
		},
		{
			name:  "resets",
			query: `resets(metric_3[5m])`,
		},
		{
			name:  "changes",
			query: `changes(metric_1[5m])`,
		},
		{
			name:  "idelta",
			query: `idelta(metric_2[5m])`,
		},
		{
			name:  "predict_linear",
			query: `predict_linear(metric_3[5m], 100)`,
		},
		{
			name:  "deriv",
			query: `deriv(metric_1[5m])`,
		},
		{
			name:  "timestamp",
			query: `timestamp(metric_2)`,
		},
		{
			name:  "timestamp timestamp",
			query: `timestamp(timestamp(metric_2))`,
		},
		{
			name:  "vector",
			query: `vector(1)`,
		},
		{
			name:  "vector time",
			query: `vector(time())`,
		},
		{
			name:  "histogram quantile non-existent",
			query: `histogram_quantile(0.9, nonexistent_metric)`,
		},
		{
			name:  "histogram quantile complex",
			query: `histogram_quantile(0.5, rate(metric_1[1m]))`,
		},
		{
			name:  "complex query 1",
			query: `sum by(instance) (metric_1) + on(instance) group_left(foo) metric_2`,
		},
		{
			name:  "complex query 2",
			query: `max_over_time((time() - max(metric_3) < 1000)[5m:10s] offset 5m)`,
		},
		{
			name:  "complex query 3",
			query: `holt_winters(metric_1[10m], 0.1, 0.5)`,
		},
	}

	runPromQLQueryTests(t, testCases, start, end)
}

type testCase struct {
	name  string
	query string
}

func runPromQLQueryTests(t *testing.T, cases []testCase, start, end time.Time) {
	steps := []time.Duration{10 * time.Second, 30 * time.Second, time.Minute, 5 * time.Minute, 30 * time.Minute}
	withDB(t, *testDatabase, func(db *pgxpool.Pool, t testing.TB) {
		// Ingest test dataset.
		dataset := generateLargeTimeseries()
		if *extendedTest {
			dataset = append(dataset, generateRealTimeseries()...)
		}
		ingestQueryTestDataset(db, t, dataset)
		// Getting a read-only connection to ensure read path is idempotent.
		readOnly := testhelpers.GetReadOnlyConnection(t, *testDatabase)
		defer readOnly.Close()

		var tester *testing.T
		var ok bool
		if tester, ok = t.(*testing.T); !ok {
			t.Fatalf("Cannot run test, not an instance of testing.T")
			return
		}

		router, err := buildRouter(readOnly)

		if err != nil {
			t.Fatalf("Cannot run test, unable to build router: %s", err)
			return
		}

		ts := httptest.NewServer(router)
		defer ts.Close()

		tsURL := fmt.Sprintf("%s/api/v1", ts.URL)
		promURL := fmt.Sprintf("http://%s:%d/api/v1", testhelpers.PromHost, testhelpers.PromPort.Int())
		client := &http.Client{Timeout: 300 * time.Second}

		start := time.Unix(startTime/1000, 0)
		end := time.Unix(endTime/1000, 0)
		var (
			requestCases []requestCase
			tsReq        *http.Request
			promReq      *http.Request
		)
		for _, c := range cases {
			tsReq, err = genInstantRequest(tsURL, c.query, start)
			if err != nil {
				t.Fatalf("unable to create TS PromQL query request: %s", err)
			}
			promReq, err = genInstantRequest(promURL, c.query, start)
			if err != nil {
				t.Fatalf("unable to create Prometheus PromQL query request: %s", err)
			}
			requestCases = append(requestCases, requestCase{tsReq, promReq, fmt.Sprintf("%s (instant query, ts=start)", c.name)})

			// Instant Query, 30 seconds after start
			tsReq, err = genInstantRequest(tsURL, c.query, start.Add(time.Second*30))
			if err != nil {
				t.Fatalf("unable to create TS PromQL query request: %s", err)
			}
			promReq, err = genInstantRequest(promURL, c.query, start.Add(time.Second*30))
			if err != nil {
				t.Fatalf("unable to create Prometheus PromQL query request: %s", err)
			}
			requestCases = append(requestCases, requestCase{tsReq, promReq, fmt.Sprintf("%s (instant query, ts=start+30sec)", c.name)})
		}
		testMethod := testRequestConcurrent(requestCases, client, queryResultComparator)
		tester.Run("test instant query endpoint", testMethod)

		requestCases = nil
		for _, c := range cases {
			for _, step := range steps {
				tsReq, err = genRangeRequest(tsURL, c.query, start, end.Add(10*time.Minute), step)
				if err != nil {
					t.Fatalf("unable to create TS PromQL range query request: %s", err)
				}
				promReq, err = genRangeRequest(promURL, c.query, start, end.Add(10*time.Minute), step)
				if err != nil {
					t.Fatalf("unable to create Prometheus PromQL range query request: %s", err)
				}
				requestCases = append(requestCases, requestCase{tsReq, promReq, fmt.Sprintf("%s (range query, step size: %s)", c.name, step.String())})
			}

			//range that straddles the end of the generated data
			for _, step := range steps {
				tsReq, err = genRangeRequest(tsURL, c.query, end, end.Add(time.Minute*10), step)
				if err != nil {
					t.Fatalf("unable to create TS PromQL range query request: %s", err)
				}
				promReq, err = genRangeRequest(promURL, c.query, end, end.Add(time.Minute*10), step)
				if err != nil {
					t.Fatalf("unable to create Prometheus PromQL range query request: %s", err)
				}
				requestCases = append(requestCases, requestCase{tsReq, promReq, fmt.Sprintf("%s (range query, step size: %s, straddles_end)", c.name, step.String())})
			}
		}
		testMethod = testRequestConcurrent(requestCases, client, queryResultComparator)
		tester.Run("test range query endpoint", testMethod)
	})
}

func queryResultComparator(promContent []byte, tsContent []byte) error {
	var got, wanted queryResponse

	err := json.Unmarshal(tsContent, &got)
	if err != nil {
		return fmt.Errorf("unexpected error returned when reading connector response body:\n%s\nbody:\n%s\n", err.Error(), tsContent)
	}

	err = json.Unmarshal(promContent, &wanted)
	if err != nil {
		return fmt.Errorf("unexpected error returned when reading Prometheus response body:\n%s\nbody:\n%s\n", err.Error(), promContent)
	}

	// Sorting to make sure
	sort.Sort(got.Data.Result)
	sort.Sort(wanted.Data.Result)

	if !got.Equal(wanted) {
		return fmt.Errorf("unexpected response:\ngot\n%+v\nwanted\n%+v", got, wanted)
	}

	return nil
}
