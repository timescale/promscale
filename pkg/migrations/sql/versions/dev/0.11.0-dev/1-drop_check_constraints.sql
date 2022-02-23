ALTER TABLE _ps_trace.span DROP CONSTRAINT span_check;
ALTER TABLE _ps_trace.span DROP CONSTRAINT span_parent_span_id_check;
ALTER TABLE _ps_trace.span DROP CONSTRAINT span_span_id_check;
ALTER TABLE _ps_trace.span DROP CONSTRAINT span_trace_state_check;
ALTER TABLE _ps_trace.span ALTER COLUMN duration_ms DROP EXPRESSION;
