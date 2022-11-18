package vacuum

import (
	"context"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func Test_runWorkers(t *testing.T) {
	// make a number of chunks
	// each chunk has a name that is one distinct ascii char
	// starting with A...
	makeChunks := func(num int) []*chunk {
		chunks := make([]*chunk, num)
		for i := 0; i < num; i++ {
			chunks[i] = &chunk{
				name: string(rune(65 + i)),
			}
		}
		return chunks
	}

	tests := []struct {
		name       string
		numWorkers int
		chunks     []*chunk
	}{
		{"1", 1, makeChunks(6)},
		{"2", 2, makeChunks(8)},
		{"3", 3, makeChunks(11)},
		{"4", 4, makeChunks(1)},
		{"5", 5, makeChunks(4)},
		{"6", 6, makeChunks(8)},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var mu sync.Mutex
			// when we "work" on a chunk, append its name to actual
			actual := make([]string, 0)
			runWorkers(context.Background(), tt.numWorkers, tt.chunks, func(ctx context.Context, id int, todo <-chan *chunk) {
				for c := range todo {
					func(ctx context.Context, id int, c *chunk) {
						mu.Lock()
						defer mu.Unlock()
						actual = append(actual, c.name)
					}(ctx, id, c)
				}
			})
			// now do the same thing without the workers
			expected := make([]string, 0)
			for _, c := range tt.chunks {
				expected = append(expected, c.name)
			}
			sort.Strings(actual)
			sort.Strings(expected)
			require.Equal(t, strings.Join(expected, ""), strings.Join(actual, ""))
		})
	}
}

func Test_every(t *testing.T) {
	// add 1 to count every 100 milliseconds
	// stop after 450 milliseconds
	// expect count to be 4
	var count = 0
	var mu sync.Mutex
	timer := time.NewTimer(time.Millisecond * 450)
	execute, kill := every(time.Millisecond*100, func(ctx context.Context) {
		mu.Lock()
		defer mu.Unlock()
		count = count + 1
	})
	go func() {
		execute()
	}()
	<-timer.C
	kill()
	mu.Lock()
	defer mu.Unlock()
	require.Equal(t, 4, count)
}

func TestValidate(t *testing.T) {
	type fields struct {
		disable      bool
		runFrequency time.Duration
		parallelism  int
	}
	tests := []struct {
		name    string
		fields  fields
		wantErr bool
	}{
		{
			name: "ok",
			fields: fields{
				runFrequency: time.Minute,
				parallelism:  6,
			},
			wantErr: false,
		},
		{
			name: "bad runFrequency",
			fields: fields{
				runFrequency: -1 * time.Minute,
				parallelism:  6,
			},
			wantErr: true,
		},
		{
			name: "bad parallelism",
			fields: fields{
				runFrequency: time.Minute,
				parallelism:  0,
			},
			wantErr: true,
		},
		{
			name: "both bad",
			fields: fields{
				runFrequency: -1 * time.Minute,
				parallelism:  -1,
			},
			wantErr: true,
		},
		{
			name: "disable both bad",
			fields: fields{
				disable:      true,
				runFrequency: -1 * time.Minute,
				parallelism:  -1,
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := &Config{
				Disable:        tt.fields.disable,
				RunFrequency:   tt.fields.runFrequency,
				MaxParallelism: tt.fields.parallelism,
			}
			if err := Validate(cfg); (err != nil) != tt.wantErr {
				t.Errorf("Validate(cfg) error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func Test_interpolate(t *testing.T) {
	// the following sql was used to generate the test cases
	/*
		select string_agg(format(
		$${
		    name: "%s",
		    args: args{
		        vacuumFreezeMinAge:     %s,
		        autovacuumFreezeMaxAge: %s,
		        maxChunkAge:            %s,
		        maxParallelism:         %s,
		    },
		    want: %s,
		}$$
		, format('maxParallelism %s maxChunkAge %s', x.max_parallelism, x.max_chunk_age)
		, x.vacuum_freeze_min_age
		, x.autovacuum_freeze_max_age
		, x.max_chunk_age
		, x.max_parallelism
		, x.want
		), E',\n' order by x.max_parallelism, x.max_chunk_age)
		from
		(
		values
		 (5, 1000, 11000, 1000,  1)
		,(5, 1000, 11000, 2000,  1)
		,(5, 1000, 11000, 3000,  2)
		,(5, 1000, 11000, 4000,  2)
		,(5, 1000, 11000, 5000,  3)
		,(5, 1000, 11000, 6000,  3)
		,(5, 1000, 11000, 7000,  4)
		,(5, 1000, 11000, 8000,  4)
		,(5, 1000, 11000, 9000,  5)
		,(5, 1000, 11000, 10000, 5)
		,(5, 1000, 11000, 11000, 5)
		,(5, 1000, 11000, 10500, 5)
		,(5, 1000, 11000, 5333,  3)
		--------------------------
		,(3, 1000, 11000, 1000,  1)
		,(3, 1000, 11000, 2000,  1)
		,(3, 1000, 11000, 3000,  1)
		,(3, 1000, 11000, 4000,  1)
		,(3, 1000, 11000, 5000,  2)
		,(3, 1000, 11000, 6000,  2)
		,(3, 1000, 11000, 7000,  2)
		,(3, 1000, 11000, 8000,  3)
		,(3, 1000, 11000, 9000,  3)
		,(3, 1000, 11000, 10000, 3)
		,(3, 1000, 11000, 11000, 3)
		,(3, 1000, 11000, 10500, 3)
		,(3, 1000, 11000, 5333,  2)
		) x(max_parallelism, vacuum_freeze_min_age, autovacuum_freeze_max_age, max_chunk_age, want)
		;
	*/
	type args struct {
		vacuumFreezeMinAge     float64
		autovacuumFreezeMaxAge float64
		maxChunkAge            float64
		maxParallelism         float64
	}
	tests := []struct {
		name string
		args args
		want int
	}{
		{
			name: "maxParallelism 3 maxChunkAge 1000",
			args: args{
				vacuumFreezeMinAge:     1000,
				autovacuumFreezeMaxAge: 11000,
				maxChunkAge:            1000,
				maxParallelism:         3,
			},
			want: 1,
		},
		{
			name: "maxParallelism 3 maxChunkAge 2000",
			args: args{
				vacuumFreezeMinAge:     1000,
				autovacuumFreezeMaxAge: 11000,
				maxChunkAge:            2000,
				maxParallelism:         3,
			},
			want: 1,
		},
		{
			name: "maxParallelism 3 maxChunkAge 3000",
			args: args{
				vacuumFreezeMinAge:     1000,
				autovacuumFreezeMaxAge: 11000,
				maxChunkAge:            3000,
				maxParallelism:         3,
			},
			want: 1,
		},
		{
			name: "maxParallelism 3 maxChunkAge 4000",
			args: args{
				vacuumFreezeMinAge:     1000,
				autovacuumFreezeMaxAge: 11000,
				maxChunkAge:            4000,
				maxParallelism:         3,
			},
			want: 1,
		},
		{
			name: "maxParallelism 3 maxChunkAge 5000",
			args: args{
				vacuumFreezeMinAge:     1000,
				autovacuumFreezeMaxAge: 11000,
				maxChunkAge:            5000,
				maxParallelism:         3,
			},
			want: 2,
		},
		{
			name: "maxParallelism 3 maxChunkAge 5333",
			args: args{
				vacuumFreezeMinAge:     1000,
				autovacuumFreezeMaxAge: 11000,
				maxChunkAge:            5333,
				maxParallelism:         3,
			},
			want: 2,
		},
		{
			name: "maxParallelism 3 maxChunkAge 6000",
			args: args{
				vacuumFreezeMinAge:     1000,
				autovacuumFreezeMaxAge: 11000,
				maxChunkAge:            6000,
				maxParallelism:         3,
			},
			want: 2,
		},
		{
			name: "maxParallelism 3 maxChunkAge 7000",
			args: args{
				vacuumFreezeMinAge:     1000,
				autovacuumFreezeMaxAge: 11000,
				maxChunkAge:            7000,
				maxParallelism:         3,
			},
			want: 2,
		},
		{
			name: "maxParallelism 3 maxChunkAge 8000",
			args: args{
				vacuumFreezeMinAge:     1000,
				autovacuumFreezeMaxAge: 11000,
				maxChunkAge:            8000,
				maxParallelism:         3,
			},
			want: 3,
		},
		{
			name: "maxParallelism 3 maxChunkAge 9000",
			args: args{
				vacuumFreezeMinAge:     1000,
				autovacuumFreezeMaxAge: 11000,
				maxChunkAge:            9000,
				maxParallelism:         3,
			},
			want: 3,
		},
		{
			name: "maxParallelism 3 maxChunkAge 10000",
			args: args{
				vacuumFreezeMinAge:     1000,
				autovacuumFreezeMaxAge: 11000,
				maxChunkAge:            10000,
				maxParallelism:         3,
			},
			want: 3,
		},
		{
			name: "maxParallelism 3 maxChunkAge 10500",
			args: args{
				vacuumFreezeMinAge:     1000,
				autovacuumFreezeMaxAge: 11000,
				maxChunkAge:            10500,
				maxParallelism:         3,
			},
			want: 3,
		},
		{
			name: "maxParallelism 3 maxChunkAge 11000",
			args: args{
				vacuumFreezeMinAge:     1000,
				autovacuumFreezeMaxAge: 11000,
				maxChunkAge:            11000,
				maxParallelism:         3,
			},
			want: 3,
		},
		{
			name: "maxParallelism 5 maxChunkAge 1000",
			args: args{
				vacuumFreezeMinAge:     1000,
				autovacuumFreezeMaxAge: 11000,
				maxChunkAge:            1000,
				maxParallelism:         5,
			},
			want: 1,
		},
		{
			name: "maxParallelism 5 maxChunkAge 2000",
			args: args{
				vacuumFreezeMinAge:     1000,
				autovacuumFreezeMaxAge: 11000,
				maxChunkAge:            2000,
				maxParallelism:         5,
			},
			want: 1,
		},
		{
			name: "maxParallelism 5 maxChunkAge 3000",
			args: args{
				vacuumFreezeMinAge:     1000,
				autovacuumFreezeMaxAge: 11000,
				maxChunkAge:            3000,
				maxParallelism:         5,
			},
			want: 2,
		},
		{
			name: "maxParallelism 5 maxChunkAge 4000",
			args: args{
				vacuumFreezeMinAge:     1000,
				autovacuumFreezeMaxAge: 11000,
				maxChunkAge:            4000,
				maxParallelism:         5,
			},
			want: 2,
		},
		{
			name: "maxParallelism 5 maxChunkAge 5000",
			args: args{
				vacuumFreezeMinAge:     1000,
				autovacuumFreezeMaxAge: 11000,
				maxChunkAge:            5000,
				maxParallelism:         5,
			},
			want: 3,
		},
		{
			name: "maxParallelism 5 maxChunkAge 5333",
			args: args{
				vacuumFreezeMinAge:     1000,
				autovacuumFreezeMaxAge: 11000,
				maxChunkAge:            5333,
				maxParallelism:         5,
			},
			want: 3,
		},
		{
			name: "maxParallelism 5 maxChunkAge 6000",
			args: args{
				vacuumFreezeMinAge:     1000,
				autovacuumFreezeMaxAge: 11000,
				maxChunkAge:            6000,
				maxParallelism:         5,
			},
			want: 3,
		},
		{
			name: "maxParallelism 5 maxChunkAge 7000",
			args: args{
				vacuumFreezeMinAge:     1000,
				autovacuumFreezeMaxAge: 11000,
				maxChunkAge:            7000,
				maxParallelism:         5,
			},
			want: 4,
		},
		{
			name: "maxParallelism 5 maxChunkAge 8000",
			args: args{
				vacuumFreezeMinAge:     1000,
				autovacuumFreezeMaxAge: 11000,
				maxChunkAge:            8000,
				maxParallelism:         5,
			},
			want: 4,
		},
		{
			name: "maxParallelism 5 maxChunkAge 9000",
			args: args{
				vacuumFreezeMinAge:     1000,
				autovacuumFreezeMaxAge: 11000,
				maxChunkAge:            9000,
				maxParallelism:         5,
			},
			want: 5,
		},
		{
			name: "maxParallelism 5 maxChunkAge 10000",
			args: args{
				vacuumFreezeMinAge:     1000,
				autovacuumFreezeMaxAge: 11000,
				maxChunkAge:            10000,
				maxParallelism:         5,
			},
			want: 5,
		},
		{
			name: "maxParallelism 5 maxChunkAge 10500",
			args: args{
				vacuumFreezeMinAge:     1000,
				autovacuumFreezeMaxAge: 11000,
				maxChunkAge:            10500,
				maxParallelism:         5,
			},
			want: 5,
		},
		{
			name: "maxParallelism 5 maxChunkAge 11000",
			args: args{
				vacuumFreezeMinAge:     1000,
				autovacuumFreezeMaxAge: 11000,
				maxChunkAge:            11000,
				maxParallelism:         5,
			},
			want: 5,
		},
		{
			name: "capped",
			args: args{
				vacuumFreezeMinAge:     1000,
				autovacuumFreezeMaxAge: 11000,
				maxChunkAge:            15000,
				maxParallelism:         10,
			},
			want: 10,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := interpolate(tt.args.vacuumFreezeMinAge, tt.args.autovacuumFreezeMaxAge, tt.args.maxChunkAge, tt.args.maxParallelism); got != tt.want {
				t.Errorf("interpolate() = %v, want %v", got, tt.want)
			}
		})
	}
}
