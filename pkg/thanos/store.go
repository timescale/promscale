package thanos

import (
	"context"
	"time"

	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/thanos-io/thanos/pkg/store/labelpb"
	"github.com/thanos-io/thanos/pkg/store/storepb"
	"github.com/timescale/promscale/pkg/promql"
)

type Storage struct {
	queryable promql.Queryable
}

func NewStorage(queryable promql.Queryable) *Storage {
	return &Storage{
		queryable: queryable,
	}
}

func (fc *Storage) Info(ctx context.Context, req *storepb.InfoRequest) (*storepb.InfoResponse, error) {
	return &storepb.InfoResponse{
		MinTime:   0,
		MaxTime:   time.Now().UnixNano() / 1000,
		StoreType: storepb.StoreType_STORE,
	}, nil
}

func (fc *Storage) Series(req *storepb.SeriesRequest, srv storepb.Store_SeriesServer) error {
	matchers, err := getMatchers(req.Matchers)
	if err != nil {
		return err
	}

	q, err := fc.queryable.Querier(srv.Context(), req.MinTime, req.MaxTime)
	if err != nil {
		return err
	}

	ss, _ := q.Select(false, nil, nil, nil, matchers...)

	for ss.Next() {
		series := ss.At()
		lbls := series.Labels()

		s := storepb.Series{Labels: make([]labelpb.ZLabel, 0, len(lbls))}
		for _, lbl := range lbls {
			s.Labels = append(s.Labels, labelpb.ZLabel(lbl))
		}

		chunk := chunkenc.NewXORChunk()
		appender, err := chunk.Appender()
		if err != nil {
			return err
		}

		var minTime, maxTime int64

		si := series.Iterator()

		for si.Next() {
			t, v := si.At()
			if minTime == 0 {
				minTime = t
			}
			maxTime = t

			appender.Append(t, v)
		}

		s.Chunks = append(
			s.Chunks,
			storepb.AggrChunk{
				MinTime: minTime,
				MaxTime: maxTime,
				Raw: &storepb.Chunk{
					Type: storepb.Chunk_Encoding(chunk.Encoding() - 1),
					Data: chunk.Bytes(),
				},
			},
		)

		seriesResponse := storepb.NewSeriesResponse(&s)

		if err := srv.Send(seriesResponse); err != nil {
			return err
		}
	}

	return nil
}

func (fc *Storage) LabelNames(ctx context.Context, req *storepb.LabelNamesRequest) (*storepb.LabelNamesResponse, error) {
	q, err := fc.queryable.Querier(ctx, req.Start, req.End)
	if err != nil {
		return nil, err
	}

	names, warnings, err := q.LabelNames()
	if err != nil {
		return nil, err
	}

	resp := &storepb.LabelNamesResponse{
		Names: names,
	}

	for _, w := range warnings {
		resp.Warnings = append(resp.Warnings, w.Error())
	}

	return resp, nil
}

func (fc *Storage) LabelValues(ctx context.Context, req *storepb.LabelValuesRequest) (*storepb.LabelValuesResponse, error) {
	q, err := fc.queryable.Querier(ctx, req.Start, req.End)
	if err != nil {
		return nil, err
	}

	values, warnings, err := q.LabelValues(req.Label)
	if err != nil {
		return nil, err
	}

	resp := &storepb.LabelValuesResponse{
		Values: values,
	}

	for _, w := range warnings {
		resp.Warnings = append(resp.Warnings, w.Error())
	}

	return resp, nil
}

func getMatchers(labelMatchers []storepb.LabelMatcher) ([]*labels.Matcher, error) {
	matchers := make([]*labels.Matcher, 0, len(labelMatchers))
	for _, labelMatcher := range labelMatchers {
		m, err := labels.NewMatcher(labels.MatchType(labelMatcher.Type), labelMatcher.Name, labelMatcher.Value)
		if err != nil {
			return nil, err
		}

		matchers = append(matchers, m)
	}

	return matchers, nil
}
