from flask import Flask, request, jsonify
from flask_cors import CORS
import os
import uuid
import psycopg2
from psycopg2.extras import RealDictCursor
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()  # this reads .env and populates os.environ

# -------------------------
# Flask app + CORS
# -------------------------
app = Flask(__name__)
CORS(app)

# -------------------------
# PostgreSQL connection
# -------------------------
POSTGRES_URI = os.environ.get("POSTGRES_URI")
if not POSTGRES_URI:
    raise RuntimeError("POSTGRES_URI environment variable is not set")


# def get_db_conn():
#     return psycopg2.connect(POSTGRES_URI, cursor_factory=RealDictCursor)

def get_db_conn():
    try:
        conn = psycopg2.connect(POSTGRES_URI, cursor_factory=RealDictCursor)
        # optional: verify with a trivial query
        with conn.cursor() as cur:
            cur.execute("SELECT 1")
            cur.fetchone()
        return conn
    except psycopg2.Error as e:
        # log or re-raise with context
        print("DB connection failed:", e)
        raise

def now_utc():
    return datetime.utcnow()


# -------------------------
# CREATE NODE
# -------------------------
@app.route("/node/create", methods=["POST"])
def create_node():
    data = request.get_json(force=True)
    name = data.get("name")
    description = data.get("description", "")
    status = data.get("status", "New")

    if not name or name.strip() == "":
        return jsonify({"error": "name is required"}), 400

    content_id = str(uuid.uuid4())
    created_at = now_utc()

    conn = get_db_conn()
    cur = conn.cursor()
    try:
        cur.execute(
            """
            INSERT INTO nodes (content_id, name, description, status, created_at)
            VALUES (%s, %s, %s, %s, %s)
            """,
            (content_id, name.strip(), description.strip(), status, created_at),
        )
        conn.commit()
    finally:
        cur.close()
        conn.close()

    return jsonify({
        "contentId": content_id,
        "name": name,
        "description": description,
        "status": status
    })


# -------------------------
# UPDATE NODE
# -------------------------
@app.route("/node/update/<contentId>", methods=["PUT"])
def update_node(contentId):
    data = request.get_json(force=True)
    name = data.get("name")
    description = data.get("description", "")
    status = data.get("status", "New")

    if not name or name.strip() == "":
        return jsonify({"error": "name is required"}), 400

    conn = get_db_conn()
    cur = conn.cursor()
    try:
        # Check if node exists
        cur.execute(
            "SELECT content_id FROM nodes WHERE content_id = %s",
            (contentId,),
        )
        existing = cur.fetchone()
        if not existing:
            return jsonify({"error": "Node not found"}), 404

        # Update node
        cur.execute(
            """
            UPDATE nodes
            SET name = %s,
                description = %s,
                status = %s
            WHERE content_id = %s
            """,
            (name.strip(), description.strip(), status, contentId),
        )
        conn.commit()
    finally:
        cur.close()
        conn.close()

    return jsonify({
        "contentId": contentId,
        "name": name,
        "description": description,
        "status": status
    })


# -------------------------
# DELETE NODE
# -------------------------
@app.route("/node/delete/<contentId>", methods=["DELETE"])
def delete_node(contentId):
    conn = get_db_conn()
    cur = conn.cursor()
    try:
        # Delete relationships (as parent or child)
        cur.execute(
            """
            DELETE FROM relationships
            WHERE parent_id = %s OR child_id = %s
            """,
            (contentId, contentId),
        )

        # Delete clicks (as source or target)
        cur.execute(
            """
            DELETE FROM clicks
            WHERE source_id = %s OR target_id = %s
            """,
            (contentId, contentId),
        )

        # Delete the node itself
        cur.execute(
            "DELETE FROM nodes WHERE content_id = %s",
            (contentId,),
        )

        conn.commit()
    finally:
        cur.close()
        conn.close()

    return jsonify({"message": "Node deleted"})


# -------------------------
# SEARCH UNRELATED NODES
# -------------------------
@app.route("/node/search_unrelated/<contentId>/<search_term>", methods=["GET"])
def search_unrelated_nodes(contentId, search_term):
    search_text = search_term.replace("_", " ")

    conn = get_db_conn()
    cur = conn.cursor()
    try:
        # All nodes matching search, excluding the parent itself
        cur.execute(
            """
            SELECT content_id AS "contentId",
                   name,
                   description,
                   status
            FROM nodes
            WHERE content_id <> %s
              AND (
                    name ILIKE %s
                 OR description ILIKE %s
              )
            """,
            (contentId, f"%{search_text}%", f"%{search_text}%"),
        )
        matching_nodes = cur.fetchall()

        if not matching_nodes:
            return jsonify({"message": "No match"}), 404

        # Existing children of parentId
        cur.execute(
            """
            SELECT child_id
            FROM relationships
            WHERE parent_id = %s
            """,
            (contentId,),
        )
        existing_children_ids = {row["child_id"] for row in cur.fetchall()}

    finally:
        cur.close()
        conn.close()

    results = [
        n for n in matching_nodes
        if n["contentId"] not in existing_children_ids
    ]

    if not results:
        return jsonify({"message": "No unrelated match"}), 404

    return jsonify(results)


# -------------------------
# GENERIC NODE SEARCH (includes existing children)
# -------------------------
@app.route("/node/search/<search_term>", methods=["GET"])
def search_nodes(search_term):
    search_text = search_term.replace("_", " ")

    conn = get_db_conn()
    cur = conn.cursor()
    try:
        cur.execute(
            """
            SELECT content_id AS "contentId",
                   name,
                   description,
                   status
            FROM nodes
            WHERE name ILIKE %s
               OR description ILIKE %s
            """,
            (f"%{search_text}%", f"%{search_text}%"),
        )
        rows = cur.fetchall()
    finally:
        cur.close()
        conn.close()

    if not rows:
        return jsonify({"message": "No match"}), 404

    return jsonify(rows)


# -------------------------
# CREATE RELATIONSHIP
# -------------------------
@app.route("/relation/create", methods=["POST"])
def create_relation():
    data = request.get_json(force=True)
    parentId = data.get("parentId")
    childId = data.get("childId")

    if not parentId or not childId:
        return jsonify({"error": "parentId and childId required"}), 400

    createdAt = now_utc()

    conn = get_db_conn()
    cur = conn.cursor()
    try:
        # Check if relationship already exists
        cur.execute(
            """
            SELECT id
            FROM relationships
            WHERE parent_id = %s AND child_id = %s
            """,
            (parentId, childId),
        )
        rel_exists = cur.fetchone()

        if rel_exists:
            # Idempotent: treat existing relationship as success
            return jsonify({"message": "Relationship exists"}), 200

        # Insert new relationship
        cur.execute(
            """
            INSERT INTO relationships (parent_id, child_id, created_at)
            VALUES (%s, %s, %s)
            """,
            (parentId, childId, createdAt),
        )
        conn.commit()
    finally:
        cur.close()
        conn.close()

    return jsonify({"message": "Relationship created"})


# -------------------------
# DELETE RELATIONSHIP
# -------------------------
@app.route("/relation/delete", methods=["DELETE"])
def delete_relation():
    data = request.get_json(force=True)
    parentId = data.get("parentId")
    childId = data.get("childId")

    conn = get_db_conn()
    cur = conn.cursor()
    try:
        # Delete clicks for that pair
        cur.execute(
            """
            DELETE FROM clicks
            WHERE source_id = %s AND target_id = %s
            """,
            (parentId, childId),
        )

        # Delete relationships for that pair
        cur.execute(
            """
            DELETE FROM relationships
            WHERE parent_id = %s AND child_id = %s
            """,
            (parentId, childId),
        )

        conn.commit()
    finally:
        cur.close()
        conn.close()

    return jsonify({"message": "Relationship deleted"})


# -------------------------
# CLICK LINK
# -------------------------
@app.route("/link/click", methods=["POST"])
def click_link():
    data = request.get_json(force=True)
    sourceId = data.get("sourceId")
    targetId = data.get("targetId")

    now = now_utc()

    conn = get_db_conn()
    cur = conn.cursor()
    try:
        # Check if click record exists
        cur.execute(
            """
            SELECT id, count
            FROM clicks
            WHERE source_id = %s AND target_id = %s
            """,
            (sourceId, targetId),
        )
        existing = cur.fetchone()

        if existing:
            # Update existing record
            cur.execute(
                """
                UPDATE clicks
                SET count = count + 1,
                    last_clicked = %s
                WHERE id = %s
                """,
                (now, existing["id"]),
            )
        else:
            # Insert new record
            cur.execute(
                """
                INSERT INTO clicks (source_id, target_id, count, first_clicked, last_clicked)
                VALUES (%s, %s, %s, %s, %s)
                """,
                (sourceId, targetId, 1, now, now),
            )

        conn.commit()
    finally:
        cur.close()
        conn.close()

    return jsonify({"message": "Click recorded"})


# -------------------------
# INBOUND STATS
# -------------------------
@app.route("/inbound_stats/<contentId>", methods=["GET"])
def inbound_stats(contentId):
    conn = get_db_conn()
    cur = conn.cursor()
    try:
        cur.execute(
            """
            SELECT source_id AS "sourceId",
                   count
            FROM clicks
            WHERE target_id = %s
            ORDER BY count DESC
            """,
            (contentId,),
        )
        clicks = cur.fetchall()
    finally:
        cur.close()
        conn.close()

    total = sum(c["count"] for c in clicks)

    return jsonify({
        "total_inbound_count": total,
        "inbound_connections": clicks
    })


# -------------------------
# OUTBOUND STATS
# -------------------------
@app.route("/outbound_stats/<contentId>", methods=["GET"])
def outbound_stats(contentId):
    conn = get_db_conn()
    cur = conn.cursor()
    try:
        cur.execute(
            """
            SELECT target_id AS "targetId",
                   count
            FROM clicks
            WHERE source_id = %s
            ORDER BY count DESC
            """,
            (contentId,),
        )
        clicks = cur.fetchall()
    finally:
        cur.close()
        conn.close()

    total = sum(c["count"] for c in clicks)

    return jsonify({
        "total_outbound_count": total,
        "outbound_connections": clicks
    })


# -----------------------------------------------------------
# GET FULL TREE (ENSURE STABLE VISUAL HIERARCHY)
# -----------------------------------------------------------
@app.route("/tree", methods=["GET"])
def get_tree():
    conn = get_db_conn()
    cur = conn.cursor()
    try:
        # 1. Nodes in exact creation order (to determine default root)
        cur.execute(
            """
            SELECT content_id AS "contentId",
                   name,
                   description,
                   status,
                   created_at
            FROM nodes
            ORDER BY created_at ASC
            """
        )
        nodes_raw = cur.fetchall()

        if not nodes_raw:
            return jsonify([])

        # 2. Fetch ALL relationships ordered by creation time (earliest link wins)
        cur.execute(
            """
            SELECT parent_id AS "parentId",
                   child_id AS "childId",
                   created_at
            FROM relationships
            ORDER BY created_at ASC
            """
        )
        rels = cur.fetchall()
    finally:
        cur.close()
        conn.close()

    relationships_raw = [(r["parentId"], r["childId"]) for r in rels]

    # Determine the stable visual parent for each child node
    visual_parent_map = {}
    for parentId, childId in relationships_raw:
        if childId not in visual_parent_map:
            # The first parent encountered for a child becomes its visual parent
            visual_parent_map[childId] = parentId

    # 3. Initialize nodes structure
    tree_nodes = {
        n["contentId"]: {
            "contentId": n["contentId"],
            "name": n["name"],
            "description": n.get("description", "") if isinstance(n, dict) else n["description"],
            "status": n.get("status", "New") if isinstance(n, dict) else n["status"],
            "children": []
        }
        for n in nodes_raw
    }

    # 4. Identify the stable root (the first node created)
    root_id = nodes_raw[0]["contentId"]

    # 5. Build the visual hierarchy (children list) using only the stable visual parent
    for childId, parentId in visual_parent_map.items():
        # Only establish a visual link if the parent exists and the child is not the root node itself
        if parentId in tree_nodes and childId != root_id:
            tree_nodes[parentId]["children"].append(childId)

    # 6. Convert the map back to a list, maintaining the original creation order
    result_tree = [tree_nodes[n["contentId"]] for n in nodes_raw]

    return jsonify(result_tree)


# -------------------------
# RESET ALL
# -------------------------
@app.route("/reset", methods=["DELETE"])
def reset_all():
    conn = get_db_conn()
    cur = conn.cursor()
    try:
        cur.execute("DELETE FROM clicks")
        cur.execute("DELETE FROM relationships")
        cur.execute("DELETE FROM nodes")
        conn.commit()
    finally:
        cur.close()
        conn.close()

    return jsonify({"message": "Reset done"})

@app.route("/stats/all", methods=["GET"])
def get_all_stats():
    conn = get_db_conn()
    try:
        with conn.cursor() as cur:
            # Get total inbound clicks per node (where node is the target)
            cur.execute("""
                SELECT target_id AS node_id, COALESCE(SUM(count), 0) AS total_inbound_count
                FROM clicks
                GROUP BY target_id
            """)
            inbound_stats = {row["node_id"]: row["total_inbound_count"] for row in cur.fetchall()}

            # Get total outbound clicks per node (where node is the source)
            cur.execute("""
                SELECT source_id AS node_id, COALESCE(SUM(count), 0) AS total_outbound_count
                FROM clicks
                GROUP BY source_id
            """)
            outbound_stats = {row["node_id"]: row["total_outbound_count"] for row in cur.fetchall()}

            # Combine all node IDs from both queries
            all_node_ids = set(inbound_stats.keys()) | set(outbound_stats.keys())
            
            # Build response with 0 as default for missing entries
            result = {
                node_id: {
                    "total_inbound_count": inbound_stats.get(node_id, 0),
                    "total_outbound_count": outbound_stats.get(node_id, 0)
                }
                for node_id in all_node_ids
            }

            return jsonify(result)
    finally:
        conn.close()
# -------------------------
# START SERVER
# -------------------------
if __name__ == "__main__":
    app.run(port=5000, debug=False)