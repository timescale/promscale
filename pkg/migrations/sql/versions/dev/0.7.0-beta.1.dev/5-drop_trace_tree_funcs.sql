DROP FUNCTION IF EXISTS ps_trace.upstream_spans(ps_trace.trace_id, bigint, int) CASCADE;
DROP FUNCTION IF EXISTS ps_trace.downstream_spans(ps_trace.trace_id, bigint, int) CASCADE;
DROP FUNCTION IF EXISTS ps_trace.span_tree(ps_trace.trace_id, bigint, int) CASCADE;
