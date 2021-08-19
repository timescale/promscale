/*
    this is just some scratch work to explore how we
    might represent the DAGs that make up traces
*/


-- adjacency model
-- each node has a "pointer" to its parent node
DROP TABLE IF EXISTS adj CASCADE;
CREATE TABLE IF NOT EXISTS adj
( id int not null primary key
, parent_id int references adj (id)
);

insert into adj (id, parent_id) values
(1, null),
(2, 1),
(3, 1),
(4, 1),
(5, 2),
(6, 2),
(7, 3),
(8, 4),
(9, 4),
(10, 2),
(11, 2),
(12, 3),
(13, 7),
(14, 4)
;

-- path enumeration model using node enumeration
DROP TABLE IF EXISTS node_path CASCADE;
CREATE TABLE IF NOT EXISTS node_path
( id int not null primary key
, node_path int[] not null
);

with recursive x(id, node_path) as
(
    select
      id
    , array_append('{}'::int[], id) as node_path
    from adj
    where parent_id is null
    union all
    select
      adj.id
    , array_append(x.node_path, adj.id) as node_path
    from x
    inner join adj on (x.id = adj.parent_id)
)
insert into node_path (id, node_path)
select *
from x
;

-- a trace table using path enumeration using node enumeration
drop table if exists trace1 cascade ;
create table trace1
( id int not null primary key
, tree jsonb not null
);

with recursive x(id, node_path) as
(
    select
      id
    , jsonb_build_array(id) as node_path
    , id as root
    from adj
    where parent_id is null
    union all
    select
      adj.id
    , x.node_path || jsonb_build_array(adj.id) as node_path
    , x.root as root
    from x
    inner join adj on (x.id = adj.parent_id)
)
insert into trace1 (id, tree)
select
  x.root
, jsonb_agg(x.node_path)
from x
group by x.root
;


with recursive x(trace_id, span_id, start_time, end_time, span_tree) as
(
    select
      trace_id
    , span_id
    , start_time
    , end_time
    , jsonb_build_array(span_id) as span_tree
    from span
    where parent_span_id is null
    union all
    select
      x.trace_id
    , s.span_id
    , least(x.start_time, s.start_time) as start_time
    , greatest(x.end_time, s.end_time) as end_time
    , x.span_tree || jsonb_build_array(s.span_id) as span_tree
    from x
    inner join span s on x.trace_id = s.trace_id
    and x.span_id = s.parent_span_id
)
select
  x.trace_id
, tstzrange(x.start_time, x.end_time, '[)') as span_time_range
, jsonb_agg(x.span_tree) as span_tree
from x
group by x.trace_id, x.start_time, x.end_time
;

