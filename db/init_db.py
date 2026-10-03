"""Create the database if missing and apply db/migrations/*.sql once each.
Usage: python -m db.init_db [--reset]   (--reset drops and recreates the schema)"""
import glob, os, sys
from urllib.parse import urlparse
import psycopg2
import config


def ensure_db():
    u = urlparse(config.DATABASE_URL)
    name = u.path.lstrip("/")
    c = psycopg2.connect(u._replace(path="/postgres").geturl())
    c.autocommit = True
    cur = c.cursor()
    cur.execute("SELECT 1 FROM pg_database WHERE datname=%s", (name,))
    if not cur.fetchone():
        cur.execute(f'CREATE DATABASE "{name}"')
        print(f"created database {name}")
    c.close()


def migrate(reset=False):
    c = psycopg2.connect(config.DATABASE_URL)
    c.autocommit = True
    cur = c.cursor()
    if reset:
        cur.execute("DROP SCHEMA public CASCADE; CREATE SCHEMA public;")
    cur.execute("CREATE TABLE IF NOT EXISTS schema_migrations(name TEXT PRIMARY KEY, applied_at TIMESTAMPTZ DEFAULT now())")
    here = os.path.join(os.path.dirname(__file__), "migrations")
    for f in sorted(glob.glob(os.path.join(here, "*.sql"))):
        name = os.path.basename(f)
        cur.execute("SELECT 1 FROM schema_migrations WHERE name=%s", (name,))
        if cur.fetchone():
            continue
        cur.execute(open(f).read())
        cur.execute("INSERT INTO schema_migrations(name) VALUES (%s)", (name,))
        print("applied", name)
    c.close()


if __name__ == "__main__":
    ensure_db()
    migrate("--reset" in sys.argv)
    print("database ready")
