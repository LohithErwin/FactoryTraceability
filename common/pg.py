import psycopg2, psycopg2.extras
import config

_c = None


def conn():
    global _c
    if _c is None or _c.closed:
        _c = psycopg2.connect(config.DATABASE_URL)
        _c.autocommit = True
    return _c


def _run(sql, params, dict_rows):
    global _c
    for attempt in (1, 2):
        try:
            factory = psycopg2.extras.RealDictCursor if dict_rows else None
            with conn().cursor(cursor_factory=factory) as cur:
                cur.execute(sql, params)
                return cur.fetchall() if dict_rows else cur.rowcount
        except (psycopg2.OperationalError, psycopg2.InterfaceError):
            _c = None
            if attempt == 2:
                raise


def execute(sql, params=None):
    return _run(sql, params, False)


def query(sql, params=None):
    return _run(sql, params, True)
