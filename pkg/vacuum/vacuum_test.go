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
	tests := []struct {
		parallelism int
		work        string
	}{
		{1, "abcdef"},
		{2, "helloworld"},
		{3, "foobarbaz12"},
		{4, "1"},
		{5, "buzz"},
		{6, "buzzbuzz"},
	}
	for _, tt := range tests {
		t.Run(tt.work, func(t *testing.T) {
			var mu sync.Mutex
			// each individual char is a unit of "work"
			// the work will consist of adding the char to worked slice
			chunks := strings.Split(tt.work, "")
			worked := make([]string, 0)
			runWorkers(context.Background(), tt.parallelism, chunks, func(ctx context.Context, id int, todo <-chan string) {
				for chunk := range todo {
					func(ctx context.Context, id int, chunk string) {
						mu.Lock()
						defer mu.Unlock()
						worked = append(worked, chunk)
					}(ctx, id, chunk)
				}
			})
			sort.Strings(chunks)
			sort.Strings(worked)
			require.Equal(t, strings.Join(chunks, ""), strings.Join(worked, ""))
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
				Disable:      tt.fields.disable,
				RunFrequency: tt.fields.runFrequency,
				Parallelism:  tt.fields.parallelism,
			}
			if err := Validate(cfg); (err != nil) != tt.wantErr {
				t.Errorf("Validate(cfg) error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
