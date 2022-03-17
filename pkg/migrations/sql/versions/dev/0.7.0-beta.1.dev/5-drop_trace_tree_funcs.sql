DROP FUNCTION IF EXISTS ps_trace.upstream_spans(UUID, bigint, int) CASCADE;
DROP FUNCTION IF EXISTS ps_trace.downstream_spans(UUID, bigint, int) CASCADE;
DROP FUNCTION IF EXISTS ps_trace.span_tree(UUID, bigint, int) CASCADE;
