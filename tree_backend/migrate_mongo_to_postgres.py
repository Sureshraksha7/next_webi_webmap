import os
import uuid
from datetime import datetime

from dotenv import load_dotenv
from pymongo import MongoClient
import psycopg2
from psycopg2.extras import execute_batch

# -------------------------
# CONFIG
# -------------------------

load_dotenv()

MONGO_URI = os.getenv("MONGO_URI")
MONGO_DB = "nextwebi_tree"  # from Atlas screenshot

POSTGRES_DSN = os.getenv("POSTGRES_URI")

if not MONGO_URI:
    raise RuntimeError("MONGO_URI is not set in .env")
if not POSTGRES_DSN:
    raise RuntimeError("POSTGRES_URI is not set in .env")


# -------------------------
# MIGRATE NODES
# -------------------------

def migrate_nodes(mongo_db, pg_conn):
    # Mongo collection: nextwebi_tree.nodes
    nodes = list(mongo_db.nodes.find({}))
    rows = []

    for doc in nodes:
        # Mongo fields: contentId, name, description, status, createdAt
        content_id = str(doc.get("contentId") or doc.get("_id") or uuid.uuid4())
        name = doc.get("name", "")
        description = doc.get("description", "")
        status = doc.get("status", "New")

        created_at = doc.get("createdAt")
        if isinstance(created_at, str):
            created_at = None
        created_at = created_at or datetime.utcnow()

        rows.append((content_id, name, description, status, created_at))

    if not rows:
        print("No nodes to migrate.")
        return

    with pg_conn:
        with pg_conn.cursor() as cur:
            execute_batch(
                cur,
                """
                INSERT INTO nodes (content_id, name, description, status, created_at)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (content_id) DO NOTHING
                """,
                rows,
            )
    print(f"Migrated {len(rows)} nodes")


# -------------------------
# MIGRATE RELATIONSHIPS
# -------------------------
# Assumes Mongo docs look like:
# { _id: ..., parentId: "<uuid>", childId: "<uuid>", createdAt: ISODate(...) }

def migrate_relationships(mongo_db, pg_conn):
    rels = list(mongo_db.relationships.find({}))
    rows = []

    for doc in rels:
        # CHANGE these keys if your Mongo field names differ
        parent_id = str(doc.get("parentId") or doc.get("parent_id"))
        child_id = str(doc.get("childId") or doc.get("child_id"))

        if not parent_id or not child_id:
            # skip malformed records
            continue

        created_at = doc.get("createdAt") or doc.get("created_at")
        if isinstance(created_at, str):
            created_at = None
        created_at = created_at or datetime.utcnow()

        rows.append((parent_id, child_id, created_at))

    if not rows:
        print("No relationships to migrate.")
        return

    with pg_conn:
        with pg_conn.cursor() as cur:
            execute_batch(
                cur,
                """
                INSERT INTO relationships (parent_id, child_id, created_at)
                VALUES (%s, %s, %s)
                """,
                rows,
            )
    print(f"Migrated {len(rows)} relationships")


# -------------------------
# MIGRATE CLICKS
# -------------------------
# Assumes Mongo docs look like:
# {
#   _id: ...,
#   sourceId: "<uuid>",
#   targetId: "<uuid>",
#   count: <int>,
#   firstClicked: ISODate(...),
#   lastClicked: ISODate(...)
# }

def migrate_clicks(mongo_db, pg_conn):
    clicks = list(mongo_db.clicks.find({}))
    rows = []

    for doc in clicks:
        # CHANGE these keys if your Mongo field names differ
        source_id = str(doc.get("sourceId") or doc.get("source_id"))
        target_id = str(doc.get("targetId") or doc.get("target_id"))

        if not source_id or not target_id:
            continue

        count = int(doc.get("count", 0))

        first_clicked = doc.get("firstClicked") or doc.get("first_clicked")
        last_clicked = doc.get("lastClicked") or doc.get("last_clicked")

        if isinstance(first_clicked, str):
            first_clicked = None
        if isinstance(last_clicked, str):
            last_clicked = None

        if not first_clicked:
            first_clicked = datetime.utcnow()
        if not last_clicked:
            last_clicked = first_clicked

        rows.append((source_id, target_id, count, first_clicked, last_clicked))

    if not rows:
        print("No clicks to migrate.")
        return

    with pg_conn:
        with pg_conn.cursor() as cur:
            execute_batch(
                cur,
                """
                INSERT INTO clicks (source_id, target_id, count, first_clicked, last_clicked)
                VALUES (%s, %s, %s, %s, %s)
                """,
                rows,
            )
    print(f"Migrated {len(rows)} clicks")


# -------------------------
# MAIN
# -------------------------

def main():
    # Connect to Mongo
    mongo_client = MongoClient(MONGO_URI)
    mongo_db = mongo_client[MONGO_DB]

    # Connect to Postgres
    pg_conn = psycopg2.connect(POSTGRES_DSN)

    try:
        migrate_nodes(mongo_db, pg_conn)
        migrate_relationships(mongo_db, pg_conn)
        migrate_clicks(mongo_db, pg_conn)
    finally:
        pg_conn.close()
        mongo_client.close()

    print("Migration completed.")


if __name__ == "__main__":
    main()