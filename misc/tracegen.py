#!/usr/bin/env python3
from typing import Dict, List, Any, Tuple, Optional
import uuid
from datetime import date, datetime, timedelta
import random
import string
import os
import json
import psycopg2
from multiprocessing import Process

NUM_WORKERS = os.environ.get("NUM_WORKERS", 4)
MIN_DEPTH = os.environ.get("MIN_DEPTH", 2)
MAX_DEPTH = os.environ.get("MAX_DEPTH", 5)
MIN_BREADTH = os.environ.get("MIN_BREADTH", 1)
MAX_BREADTH = os.environ.get("MAX_BREADTH", 3)

SPAN_TAG_TYPE = 1
RESOURCE_TAG_TYPE = 2

std_tag_key_set = {}
std_tag_key_list = []


class Resource:
    tags: Dict[str, Any]
    dropped_tags_count: int
    schema_url: str


class InstrumentationLib:
    name: str
    version: str
    schema_url: str


class Span:
    trace_id: uuid
    span_id: int
    trace_state: str
    parent_span_id: int
    name: str
    span_kind: str
    start_time: datetime
    end_time: datetime
    duration_ms: float
    span_tags: Dict[str, Any]
    dropped_tags_count: int
    dropped_events_count: int
    dropped_link_count: int
    status_code: str
    status_message: str
    instrumentation_lib: InstrumentationLib
    resource: Resource


class Trace:
    trace_id: uuid
    spans: List[Span]


def generate_tags() -> Dict[str, Any]:
    tags = {}
    # service.name is "required" per spec
    tags['service.name'] = random.choice(string.ascii_lowercase + string.ascii_uppercase) * random.randint(1, 5)
    for _ in range(random.randint(3, 12)):
        which = random.choice(['fake', 'standard'])
        if which == 'fake':
            kind = random.choice(['int', 'bool', 'date', 'text'])
            num = random.randint(1, 50)
            k = f"{kind}{num}"
            if k in tags:
                continue
            if kind == 'int':
                v = random.randint(1, 50)
            elif kind == 'bool':
                v = random.choice([True, False])
            elif kind == 'date':
                v = (date(2021, 1, 1) + timedelta(days=random.randint(0, 365))).isoformat()
            else:
                v = random.choice(string.ascii_lowercase + string.ascii_uppercase) * random.randint(1, 5)
            tags[k] = v
        else:
            k = random.choice(std_tag_key_list)
            if k in tags:
                continue
            v = random.choice(string.ascii_lowercase + string.ascii_uppercase) * random.randint(1, 5)
            tags[k] = v
    return tags


def generate_resource() -> Resource:
    i = random.randint(1, 20)
    resource = Resource()
    resource.tags = generate_tags()
    resource.schema_url = f"service{i}.resource.example"
    resource.dropped_tags_count = random.choice([0, 0, 0, 1, 2])
    return resource


def generate_instrumentation_lib() -> InstrumentationLib:
    i = random.randint(1, 20)
    instrumentation_lib = InstrumentationLib()
    instrumentation_lib.name = f"lib{i}"
    instrumentation_lib.version = "1.2.3"
    instrumentation_lib.schema_url = f"lib{i}.instrumentation.example"
    return instrumentation_lib


def generate_span_kind() -> str:
    return random.choice([
        'unspecified',
        'internal',
        'server',
        'client',
        'producer',
        'consumer'])


def generate_status_code() -> str:
    return random.choice(['unset', 'ok', 'error'])


def generate_span(trace: Trace, parent_span: Optional[Span], depth: int, child: int, siblings: int, min_breadth: int, max_breadth: int) -> None:
    span = Span()
    span.trace_id = trace.trace_id
    span.span_id = random.getrandbits(63)
    if parent_span is None:
        span.parent_span_id = None
        duration_ms = random.randint(50, 5000)
        span.start_time = datetime.now()
        span.end_time = span.start_time + timedelta(milliseconds=duration_ms)
        span.duration_ms = duration_ms
    else:
        span.parent_span_id = parent_span.span_id
        p = parent_span.end_time - parent_span.start_time
        d = p / siblings
        span.start_time = parent_span.end_time + (d * child)
        span.end_time = span.start_time + d
        span.duration_ms = (span.end_time - span.start_time).total_seconds() * 1000.0
    span.trace_state = f"trace_state{random.randint(1, 4)}"
    span.name = 'r' if parent_span is None else f"{parent_span.name}.f{child}"
    span.span_kind = generate_span_kind()
    span.span_tags = generate_tags()
    span.dropped_tags_count = random.choice([0, 0, 0, 1, 2])
    span.dropped_events_count = random.choice([0, 0, 0, 1, 2])
    span.dropped_link_count = random.choice([0, 0, 0, 1, 2])
    span.status_code = generate_status_code()
    span.status_message = span.status_code
    span.instrumentation_lib = generate_instrumentation_lib()
    span.resource = generate_resource()
    trace.spans.append(span)
    depth = depth - 1
    if depth > 0:
        siblings = random.randint(min_breadth, max_breadth)
        for child in range(siblings):
            generate_span(trace, span, depth, child, siblings, min_breadth, max_breadth)


def generate_trace(min_depth: int, max_depth: int, min_breadth: int, max_breadth: int) -> Trace:
    trace = Trace()
    trace.trace_id = uuid.uuid4()
    trace.spans = []
    depth = random.randint(min_depth, max_depth)
    generate_span(trace, None, depth, 0, 0, min_breadth, max_breadth)  # recursively build spans
    return trace


def save_tag_keys(tag_keys: List[Tuple[str, int]], cur) -> None:
    to_save = []
    for t in tag_keys:
        if t[0] not in std_tag_key_set:  # don't bother saving standard tag keys, we know they are already there
            to_save.append(t)
    if len(to_save) == 0:
        return
    to_save.sort(key=lambda tup: tup[0])  # insert in sorted order to prevent deadlocks
    for tup in to_save:
        cur.execute(f"select ps_trace.put_tag_key(%s, %s::ps_trace.tag_type)", tup)


def save_tags(tags: List[Tuple[str, Any, int]], cur) -> None:
    tags.sort(key=lambda tup: (tup[0], tup[1]))  # insert in sorted order to prevent deadlocks
    for tag in tags:
        # to_jsonb takes anyelement and gets confused on text without an explicit cast
        x = 'to_jsonb(%s::text)' if type(tag[1]) == str else 'to_jsonb(%s)'
        cur.execute(f"select ps_trace.put_tag(%s, {x}, %s::ps_trace.tag_type)", tag)


def save_instrumentation_lib(instrumentation_lib: InstrumentationLib, cur) -> None:
    cur.execute(
        'insert into _ps_trace.schema_url (url) values (%s) on conflict (url) do nothing',
        (instrumentation_lib.schema_url,))
    cur.execute('''
        insert into _ps_trace.instrumentation_lib (name, version, schema_url_id)
        select %s, %s, (select id from _ps_trace.schema_url where url = %s limit 1)
        on conflict (name, version, schema_url_id) do nothing
        ''', (instrumentation_lib.name, instrumentation_lib.version, instrumentation_lib.schema_url))


def save_span_name(service_name: str, span_name: str, span_kind: str, cur) -> int:
    cur.execute('select ps_trace.put_operation(%s, %s, %s)', (service_name, span_name, span_kind))
    return cur.fetchone()[0]


def save_span(span: Span, cur) -> None:
    sql = '''
    insert into _ps_trace.schema_url (url)
    values (%s)
    on conflict (url) do nothing'''
    cur.execute(sql, (span.resource.schema_url,))
    sql = '''
    insert into _ps_trace.span
    (
        trace_id,
        span_id,
        trace_state,
        parent_span_id,
        operation_id,
        start_time,
        end_time,
        duration_ms,
        span_tags,
        dropped_tags_count,
        dropped_events_count,
        dropped_link_count,
        status_code,
        status_message,
        instrumentation_lib_id,
        resource_tags,
        resource_dropped_tags_count,
        resource_schema_url_id
    )
    select
        %(trace_id)s,
        %(span_id)s,
        %(trace_state)s,
        %(parent_span_id)s,
        (
            select n.id
            from _ps_trace.operation n
            inner join _ps_trace.tag t on (t.key = 'service.name' and n.service_name_id = t.id)
            where n.span_name = %(span_name)s
            and n.span_kind = %(span_kind)s
            and t.value = to_jsonb(%(service_name)s::text)
            limit 1
        ),
        %(start_time)s,
        %(end_time)s,
        %(duration_ms)s,
        ps_trace.get_tag_map(%(span_tags)s),
        %(dropped_tags_count)s,
        %(dropped_events_count)s,
        %(dropped_link_count)s,
        %(status_code)s,
        %(status_message)s,
        (select id from _ps_trace.instrumentation_lib where name = %(instrumentation_lib)s limit 1),
        ps_trace.get_tag_map(%(resource_tags)s),
        %(resource_dropped_tags_count)s,
        (select id from _ps_trace.schema_url where url = %(resource_schema_url)s limit 1)
    '''
    cur.execute(sql, {
        'trace_id': span.trace_id.hex,
        'span_id': span.span_id,
        'trace_state': span.trace_state,
        'parent_span_id': span.parent_span_id,
        'service_name': span.resource.tags['service.name'],
        'span_name': span.name,
        'span_kind': span.span_kind,
        'start_time': span.start_time,
        'end_time': span.end_time,
        'duration_ms': span.duration_ms,
        'span_tags': json.dumps(span.span_tags),
        'dropped_tags_count': span.dropped_tags_count,
        'dropped_events_count': span.dropped_events_count,
        'dropped_link_count': span.dropped_link_count,
        'status_code': span.status_code,
        'status_message': span.status_message,
        'instrumentation_lib': span.instrumentation_lib.name,
        'resource_tags': json.dumps(span.resource.tags),
        'resource_dropped_tags_count': span.resource.dropped_tags_count,
        'resource_schema_url': span.resource.schema_url
    })


def save_trace(trace: Trace, cur, con) -> int:
    for span in trace.spans:
        # save span tag keys and resource tag keys
        keys = [(k, SPAN_TAG_TYPE) for k in span.span_tags.keys()]
        keys.extend([(k, RESOURCE_TAG_TYPE) for k in span.resource.tags.keys()])
        save_tag_keys(keys, cur)
        # save span tags and resource tags
        tags = [(k, v, SPAN_TAG_TYPE) for k, v in span.span_tags.items()]
        tags.extend([(k, v, RESOURCE_TAG_TYPE) for k, v in span.resource.tags.items()])
        save_tags(tags, cur)
        save_instrumentation_lib(span.instrumentation_lib, cur)
        save_span_name(span.resource.tags['service.name'], span.name, span.span_kind, cur)
        save_span(span, cur)
        con.commit()
    return len(trace.spans)


def load_standard_tags(cur) -> None:
    global std_tag_key_set, std_tag_key_list
    cur.execute('select key from _ps_trace.tag_key where id <= 174')
    std_tag_key_list = [r[0] for r in cur]
    std_tag_key_set = {k for k in std_tag_key_list}


def run(worker_id: int) -> None:
    print(f"worker {worker_id} connecting to database...")
    with psycopg2.connect(os.environ['DATABASE_URL']) as con:
        with con.cursor() as cur:
            load_standard_tags(cur)
            print(f"worker {worker_id} generating traces...")
            spans_count = 0
            while True:
                before = int(spans_count / 1000)
                spans_count += save_trace(generate_trace(MIN_DEPTH, MAX_DEPTH, MIN_BREADTH, MAX_BREADTH), cur, con)
                if int(spans_count / 1000) > before:
                    print(f"worker {worker_id} spans generated: {spans_count}")


def main() -> None:
    assert 'DATABASE_URL' in os.environ
    workers = []
    for worker_id in range(NUM_WORKERS):
        worker = Process(target=run, args=(worker_id,))
        worker.start()
        workers.append(worker)
    for worker in workers:
        worker.join()


if __name__ == '__main__':
    main()
