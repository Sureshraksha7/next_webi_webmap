import os
from dotenv import load_dotenv
import psycopg2

# Load POSTGRES_URI from .env
load_dotenv()
dsn = os.getenv("POSTGRES_URI")
if not dsn:
    raise RuntimeError("POSTGRES_URI is not set")

schema_sql = """
CREATE TABLE IF NOT EXISTS nodes (
    content_id   UUID PRIMARY KEY,
    name         TEXT NOT NULL,
    description  TEXT DEFAULT '',
    status       TEXT DEFAULT 'New',
    created_at   TIMESTAMPTZ NOT NULL
);

CREATE TABLE IF NOT EXISTS relationships (
    id          SERIAL PRIMARY KEY,
    parent_id   UUID NOT NULL,
    child_id    UUID NOT NULL,
    created_at  TIMESTAMPTZ NOT NULL
);

CREATE TABLE IF NOT EXISTS clicks (
    id            SERIAL PRIMARY KEY,
    source_id     UUID NOT NULL,
    target_id     UUID NOT NULL,
    count         INTEGER NOT NULL,
    first_clicked TIMESTAMPTZ NOT NULL,
    last_clicked  TIMESTAMPTZ NOT NULL
);
"""

def main():
    conn = psycopg2.connect(dsn)
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute(schema_sql)
        print("Tables created/verified successfully.")
    finally:
        conn.close()

if __name__ == "__main__":
    main()