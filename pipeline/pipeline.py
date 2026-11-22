#!/usr/bin/env python3
"""Exactness-first Shopping Sessions implementation.

Stage strictly parsed CSV into DuckDB-friendly, validated, delimited files.
Resolve transitive identities with DSU, then use SQL for exact aggregation.
This baseline is NOT a sub-second optimized engine.
"""
import csv
import datetime as dt
import glob
import json
import os
import subprocess
import re
import sys
import tempfile
import time
from pathlib import Path

import duckdb

DATA = Path(os.getenv('DATA_DIR', '/data'))
OUT = Path(os.getenv('OUTPUT_DIR', '/output'))
SCRATCH = Path(os.getenv('SCRATCH_DIR', '/scratch'))
NCPU = int(os.getenv('NCPU', '8'))
MEM = int(os.getenv('MEM_GB', '24'))
DAY = 86400000
WEEK = 7 * DAY
EPOCH = dt.datetime(1970, 1, 1, tzinfo=dt.timezone.utc)
F1 = re.compile(r'^\d{4}-\d\d-\d\dT\d\d:\d\d:\d\dZ$', re.ASCII)
F2 = re.compile(r'^\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d[+-]\d\d:\d\d$', re.ASCII)
F3 = re.compile(r'^[0-9]+$', re.ASCII)


def stamp(s):
    if F3.fullmatch(s):
        # Out-of-range F3 values are not valid for the dataset window.
        if len(s) > 19:
            s = s.lstrip('0') or '0'
        if len(s) > 19:
            return None
        x = int(s)
        return x if x <= 9223372036854775807 else None
    if F1.fullmatch(s):
        try:
            z = dt.datetime.strptime(s, '%Y-%m-%dT%H:%M:%SZ').replace(tzinfo=dt.timezone.utc)
            return int((z-EPOCH).total_seconds())*1000
        except ValueError:
            return None
    if F2.fullmatch(s):
        hh, mm = int(s[-5:-3]), int(s[-2:])
        if hh > 23 or mm > 59:
            return None
        try:
            z = dt.datetime.strptime(s[:19], '%Y-%m-%dT%H:%M:%S').replace(tzinfo=dt.timezone.utc)
            offset = (hh*60 + mm)*60000 * (1 if s[-6] == '+' else -1)
            return int((z-EPOCH).total_seconds())*1000 - offset
        except ValueError:
            return None
    return None


def fields(raw, n):
    # Exactly one LF ends a row. CR is ordinary data, not a record terminator.
    if raw.endswith(b'\n'):
        raw = raw[:-1]
    if not raw:
        return None
    try:
        cols = raw.decode('utf-8').split(',')
    except UnicodeDecodeError:
        return None
    if len(cols) != n:
        return None
    result = []
    for s in cols:
        if s.startswith('"') and s.endswith('"') and len(s) >= 2:
            s = s[1:-1]
            if '"' in s:
                return None
        elif '"' in s:
            return None
        result.append(s)
    return result


def rows(folder, n):
    for filename in sorted(glob.glob(str(DATA / folder / '*.csv'))):
        with open(filename, 'rb') as fh:
            next(fh, None)  # CSV header, never data
            for raw in fh:
                f = fields(raw, n)
                if f is not None:
                    yield f


class DSU:
    def __init__(self):
        self.parent = {}

    def root(self, x):
        par = self.parent
        if x not in par:
            par[x] = x
            return x
        while par[x] != x:
            par[x] = par[par[x]]
            x = par[x]
        return x

    def join(self, a, b):
        a, b = self.root(a), self.root(b)
        if a != b:
            if a < b: self.parent[b] = a
            else: self.parent[a] = b


def stage_writer(path):
    fh = open(path, 'w', encoding='utf8', newline='')
    return fh, csv.writer(fh, delimiter='\x1f', quotechar='"', lineterminator='\n')


def main():
    t0 = time.monotonic()
    SCRATCH.mkdir(parents=True, exist_ok=True)
    OUT.mkdir(parents=True, exist_ok=True)
    manifest = json.loads((DATA/'manifest.json').read_text())
    start, end = stamp(manifest['window_start']), stamp(manifest['window_end'])
    if start is None or end is None:
        raise ValueError('bad manifest')

    mapping = SCRATCH/'mapping.stage'
    ev_path = SCRATCH/'events.stage'
    click_path = SCRATCH/'clicks.stage'
    executable = Path(__file__).with_name('stage.exe' if os.name=='nt' else 'stage')
    use_compiled_staging = executable.exists()
    profiling = os.environ.get('SHOPPING_PROFILE') == '1'
    if use_compiled_staging:
        staged = tempfile.TemporaryDirectory(prefix='shopping-', dir=SCRATCH)
        stage_dir = Path(staged.name)
        mapping = stage_dir / 'mapping.stage'
        stage_begin = time.perf_counter()
        stage_env = dict(os.environ, SCRATCH_DIR=staged.name)
        subprocess.run([str(executable)], env=stage_env, check=True)
        event_paths = sorted(stage_dir.glob('events-*.stage'))
        click_paths = sorted(stage_dir.glob('clicks-*.stage'))
        if profiling:
            print(f'[PROFILE] Rust input staging: {time.perf_counter()-stage_begin:.3f}s', file=sys.stderr, flush=True)

    if not use_compiled_staging:
        dsu = DSU()
        for a, b in rows('links', 2):
            if a and b: dsu.join(a, b)

        mapping = SCRATCH/'mapping.stage'
        h,w = stage_writer(mapping)
        for uid in dsu.parent:
            w.writerow((uid, dsu.root(uid)))
        h.close()
        del dsu

        ev_path = SCRATCH/'events.stage'
        h, w = stage_writer(ev_path)
        for event_id, user_id, ts, ingest, kind, product, price in rows('events', 7):
            if not event_id: continue
            # Validation is deliberately after event_id deduplication in SQL.
            ms = stamp(ts)
            ing = stamp(ingest)
            if ing is None:
                raise ValueError('ingest_ts promised valid by specification')
            cents = int(price) if F3.fullmatch(price) and int(price) <= 10000000 and int(price) >= 1 else None
            w.writerow((event_id, user_id, ms, ing, kind, product, cents))
        h.close()

        click_path = SCRATCH/'clicks.stage'
        h,w = stage_writer(click_path)
        for click_id, user_id, campaign, ts in rows('clicks', 4):
            if not click_id: continue
            w.writerow((click_id, user_id, campaign, stamp(ts)))
        h.close()
        event_paths = [ev_path]
        click_paths = [click_path]

    con = duckdb.connect()
    con.execute(f'SET threads = {max(1,NCPU)}')
    con.execute(f"SET memory_limit = '{max(1,int(MEM*.8))}GB'")
    con.execute(f"SET temp_directory = '{(SCRATCH/'duckdb_tmp').as_posix()}'")
    con.execute('SET preserve_insertion_order = false')
    if profiling:
        class TimedConnection:
            def __init__(self, raw):
                self.raw = raw
            def execute(self, sql, *args, **kwargs):
                name = re.search(r'CREATE TEMP TABLE\s+([a-z_]+)', sql, re.I)
                drop = re.search(r'DROP TABLE\s+([a-z_]+)', sql, re.I)
                if name:
                    label = 'SQL table ' + name.group(1)
                elif drop:
                    label = 'SQL drop ' + drop.group(1)
                elif re.match(r'\s*COPY\s*\(', sql, re.I):
                    label = 'Parquet output'
                else:
                    label = 'SQL statement'
                began = time.perf_counter()
                try:
                    return self.raw.execute(sql, *args, **kwargs)
                finally:
                    print(f'[PROFILE] {label}: {time.perf_counter()-began:.3f}s', file=sys.stderr, flush=True)
        con = TimedConnection(con)

    def load(name, paths, column_types):
        cols = ', '.join("'" + k + "': '" + v + "'" for k,v in column_types.items())
        sql = f"SELECT * FROM read_csv(?, delim='{chr(31)}', header=false, auto_detect=false, columns={{{cols}}}, null_padding=false, strict_mode=true)"
        con.execute(f'CREATE TEMP TABLE {name} AS {sql}', [paths])

    def load_or_empty(name, paths, columns):
        nonempty = [path.as_posix() for path in paths if path.stat().st_size]
        if not nonempty:
            exprs = ', '.join(f'NULL::{v} AS {k}' for k,v in columns.items())
            con.execute(f'CREATE TEMP TABLE {name} AS SELECT {exprs} WHERE false')
        else:
            load(name, nonempty, columns)

    load_or_empty('identity',[mapping], {'uid':'VARCHAR', 'person':'VARCHAR'})
    load_or_empty('events_raw',event_paths, {'eid':'VARCHAR','uid':'VARCHAR','ms':'BIGINT','ing':'BIGINT','kind':'VARCHAR','product':'VARCHAR','price':'BIGINT'})
    load_or_empty('clicks_raw',click_paths, {'cid':'VARCHAR','uid':'VARCHAR','campaign':'VARCHAR','ms':'BIGINT'})
    if use_compiled_staging:
        staged.cleanup()

    con.execute(f"CREATE TEMP TABLE products AS SELECT * FROM read_csv('{(DATA/'products.csv').as_posix()}', all_varchar=true, header=true)")
    con.execute(f"CREATE TEMP TABLE campaigns AS SELECT * FROM read_csv('{(DATA/'campaigns.csv').as_posix()}', all_varchar=true, header=true)")

    con.execute(f'''
      CREATE TEMP TABLE ev AS
      WITH picked AS (
        SELECT *, row_number() OVER (PARTITION BY eid ORDER BY ing) AS rn
        FROM events_raw
      )
      SELECT coalesce(i.person, e.uid) AS person, e.ms, e.kind,
             coalesce(p.category,'unknown') AS category,
             CASE WHEN e.kind='purchase' THEN e.price ELSE 0 END AS revenue
      FROM picked e
      LEFT JOIN identity i ON i.uid=e.uid
      LEFT JOIN products p ON p.product_id=e.product
      WHERE rn=1 AND e.uid IS NOT NULL AND e.uid<>''
        AND e.ms >= {start} AND e.ms < {end}
        AND e.kind IN ('view','cart','purchase')
        AND e.product IS NOT NULL AND e.product<>''
        AND (e.kind<>'purchase' OR e.price BETWEEN 1 AND 10000000)
        AND e.ing - e.ms <= {DAY}
    ''')
    con.execute('DROP TABLE events_raw')
    con.execute('''
      CREATE TEMP TABLE session_events AS
      WITH gaps AS (
        SELECT *, CASE WHEN ms - lag(ms) OVER (PARTITION BY person ORDER BY ms) > 1800000
                       OR lag(ms) OVER (PARTITION BY person ORDER BY ms) IS NULL
                       THEN 1 ELSE 0 END AS new_session
        FROM ev
      )
      SELECT *, sum(new_session) OVER (PARTITION BY person ORDER BY ms
          ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS sn
      FROM gaps
    ''')
    con.execute('DROP TABLE ev')
    con.execute('''
      CREATE TEMP TABLE sessions AS
      SELECT person, sn, min(ms) AS start_ms, count(*) AS event_count,
             sum(revenue)::BIGINT AS revenue, count(*) FILTER (WHERE kind='purchase') AS purchase_count
      FROM session_events GROUP BY person, sn
    ''')
    con.execute('''
      CREATE TEMP TABLE session_days AS
      SELECT *, cast(epoch_ms(start_ms) AS DATE) AS day
      FROM sessions
    ''')
    con.execute('''
      CREATE TEMP TABLE user_summary AS
      SELECT person, day, count(*)::BIGINT AS sessions, sum(event_count)::BIGINT AS events,
             sum(revenue)::BIGINT AS revenue_cents
      FROM session_days GROUP BY person, day
    ''')
    con.execute('''
      CREATE TEMP TABLE category_summary AS
      SELECT s.person, s.day, e.category, sum(e.revenue)::BIGINT AS category_revenue
      FROM session_events e JOIN session_days s ON e.person=s.person AND e.sn=s.sn
      WHERE e.kind='purchase'
      GROUP BY s.person, s.day, e.category
    ''')
    con.execute('''
      CREATE TEMP TABLE top_categories AS
      SELECT person, day, category FROM (
        SELECT *, row_number() OVER (PARTITION BY person, day
           ORDER BY category_revenue DESC, category ASC) AS rn
        FROM category_summary
      ) WHERE rn=1
    ''')

    con.execute(f'''
      CREATE TEMP TABLE valid_clicks AS
      WITH dedup AS (SELECT *, row_number() OVER (PARTITION BY cid ORDER BY cid) AS rn FROM clicks_raw)
      SELECT coalesce(i.person,c.uid) AS person,c.cid,c.campaign,c.ms
      FROM dedup c LEFT JOIN identity i ON c.uid=i.uid
      JOIN campaigns m ON c.campaign=m.campaign_id
      WHERE c.rn=1 AND c.uid IS NOT NULL AND c.uid<>''
          AND c.ms >= {start-WEEK} AND c.ms < {end}
    ''')
    con.execute('DROP TABLE clicks_raw')
    con.execute('DROP TABLE identity')
    con.execute('''
      CREATE TEMP TABLE click_times AS
      SELECT person, ms, cid, campaign FROM (
        SELECT *, row_number() OVER (PARTITION BY person, ms ORDER BY cid ASC) AS rn
        FROM valid_clicks
      ) WHERE rn=1
    ''')
    con.execute(f'''
      CREATE TEMP TABLE credit AS
      SELECT s.person,s.sn,s.day,s.revenue,s.purchase_count,
             CASE WHEN c.ms >= s.start_ms - {WEEK} THEN c.campaign ELSE 'organic' END AS campaign
      FROM session_days s
      ASOF LEFT JOIN click_times c ON s.person=c.person AND s.start_ms>=c.ms
    ''')
    # Deduplicate by session and campaign before counting assists.
    con.execute(f'''
      CREATE TEMP TABLE assists_daily AS
      SELECT day,campaign,count(*)::BIGINT AS assisted_sessions
      FROM (
        SELECT DISTINCT s.person,s.sn,s.day,c.campaign
        FROM session_days s JOIN valid_clicks c
          ON c.person=s.person AND c.ms>=s.start_ms-{WEEK} AND c.ms<=s.start_ms
      ) a GROUP BY day,campaign
    ''')
    con.execute('''
      CREATE TEMP TABLE credited_daily AS
      SELECT day,campaign,count(*)::BIGINT AS sessions,
             count(*) FILTER (WHERE purchase_count>0)::BIGINT AS purchasing_sessions,
             sum(revenue)::BIGINT AS revenue_cents
      FROM credit GROUP BY day,campaign
    ''')

    for folder in ('user_daily','campaign_daily'):
        (OUT/folder).mkdir(exist_ok=True)
    con.execute(f'''
       COPY (
         SELECT u.person AS user_id,u.day,u.sessions,u.events,u.revenue_cents,
                t.category AS top_category
         FROM user_summary u LEFT JOIN top_categories t
           ON u.person=t.person AND u.day=t.day
       ) TO '{(OUT/'user_daily'/'part-0.parquet').as_posix()}' (FORMAT PARQUET)
    ''')
    con.execute(f'''
       COPY (
         SELECT coalesce(x.campaign,a.campaign) AS campaign_id,
                CASE WHEN coalesce(x.campaign,a.campaign)='organic' THEN 'organic'
                     ELSE m.channel END AS channel,
                coalesce(x.day,a.day) AS day,
                coalesce(x.sessions,0)::BIGINT AS sessions,
                coalesce(x.purchasing_sessions,0)::BIGINT AS purchasing_sessions,
                coalesce(x.revenue_cents,0)::BIGINT AS revenue_cents,
                coalesce(a.assisted_sessions,0)::BIGINT AS assisted_sessions
         FROM credited_daily x FULL OUTER JOIN assists_daily a
           ON x.campaign=a.campaign AND x.day=a.day
         LEFT JOIN campaigns m ON m.campaign_id=coalesce(x.campaign,a.campaign)
       ) TO '{(OUT/'campaign_daily'/'part-0.parquet').as_posix()}' (FORMAT PARQUET)
    ''')
    print(f'Finished in {time.monotonic()-t0:.2f}s', file=sys.stderr)


if __name__=='__main__':
    main()
