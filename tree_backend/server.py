from flask import Flask, request, jsonify
from flask_cors import CORS
from pymongo import MongoClient, ASCENDING
import os
import uuid
from datetime import datetime
from dotenv import load_dotenv
load_dotenv()  # this reads .env and populates os.environ

# -------------------------
# Flask app + CORS
# -------------------------
app = Flask(__name__)
CORS(app)

# -------------------------
# MongoDB connection
# -------------------------
MONGO_URI = os.environ.get("MONGO_URI")
if not MONGO_URI:
    raise RuntimeError("MONGO_URI environment variable is not set")

client = MongoClient(MONGO_URI)
db = client["nextwebi_tree"]  # database name

nodes_col = db["nodes"]
relationships_col = db["relationships"]
clicks_col = db["clicks"]


def now_iso():
    return datetime.utcnow().isoformat()


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
    created_at = now_iso()

    doc = {
        "contentId": content_id,
        "name": name.strip(),
        "description": description.strip(),
        "status": status,
        "createdAt": created_at,
    }
    nodes_col.insert_one(doc)

    # Return same shape as before (without createdAt)
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

    existing = nodes_col.find_one({"contentId": contentId})
    if not existing:
        return jsonify({"error": "Node not found"}), 404

    nodes_col.update_one(
        {"contentId": contentId},
        {"$set": {
            "name": name.strip(),
            "description": description.strip(),
            "status": status
        }}
    )

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
    relationships_col.delete_many({"$or": [
        {"parentId": contentId},
        {"childId": contentId}
    ]})
    clicks_col.delete_many({"$or": [
        {"sourceId": contentId},
        {"targetId": contentId}
    ]})
    nodes_col.delete_one({"contentId": contentId})
    return jsonify({"message": "Node deleted"})


# -------------------------
# SEARCH UNRELATED NODES
# -------------------------
@app.route("/node/search_unrelated/<contentId>/<search_term>", methods=["GET"])
def search_unrelated_nodes(contentId, search_term):
    search_text = search_term.replace("_", " ")
    regex = {"$regex": search_text, "$options": "i"}

    # All nodes matching search, excluding the parent itself
    matching_cursor = nodes_col.find(
        {
            "contentId": {"$ne": contentId},
            "$or": [
                {"name": regex},
                {"description": regex}
            ]
        },
        {"_id": 0, "contentId": 1, "name": 1, "description": 1, "status": 1}
    )
    matching_nodes = list(matching_cursor)

    if not matching_nodes:
        return jsonify({"message": "No match"}), 404

    # Existing children of parentId
    existing_children_cursor = relationships_col.find(
        {"parentId": contentId},
        {"_id": 0, "childId": 1}
    )
    existing_children_ids = {r["childId"] for r in existing_children_cursor}

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
    regex = {"$regex": search_text, "$options": "i"}

    cursor = nodes_col.find(
        {
            "$or": [
                {"name": regex},
                {"description": regex}
            ]
        },
        {"_id": 0, "contentId": 1, "name": 1, "description": 1, "status": 1}
    )
    rows = list(cursor)

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

    createdAt = now_iso()

    rel_exists = relationships_col.find_one({
        "parentId": parentId,
        "childId": childId
    })

    if rel_exists:
        # Idempotent: treat existing relationship as success
        return jsonify({"message": "Relationship exists"}), 200

    relationships_col.insert_one({
        "parentId": parentId,
        "childId": childId,
        "createdAt": createdAt
    })

    return jsonify({"message": "Relationship created"})


# -------------------------
# DELETE RELATIONSHIP
# -------------------------
@app.route("/relation/delete", methods=["DELETE"])
def delete_relation():
    data = request.get_json(force=True)
    parentId = data.get("parentId")
    childId = data.get("childId")

    clicks_col.delete_many({"sourceId": parentId, "targetId": childId})
    relationships_col.delete_many({"parentId": parentId, "childId": childId})

    return jsonify({"message": "Relationship deleted"})


# -------------------------
# CLICK LINK
# -------------------------
@app.route("/link/click", methods=["POST"])
def click_link():
    data = request.get_json(force=True)
    sourceId = data.get("sourceId")
    targetId = data.get("targetId")

    now = now_iso()

    existing = clicks_col.find_one({"sourceId": sourceId, "targetId": targetId})

    if existing:
        clicks_col.update_one(
            {"_id": existing["_id"]},
            {
                "$inc": {"count": 1},
                "$set": {"lastClicked": now}
            }
        )
    else:
        clicks_col.insert_one({
            "sourceId": sourceId,
            "targetId": targetId,
            "count": 1,
            "firstClicked": now,
            "lastClicked": now
        })

    return jsonify({"message": "Click recorded"})


# -------------------------
# INBOUND STATS
# -------------------------
@app.route("/inbound_stats/<contentId>", methods=["GET"])
def inbound_stats(contentId):
    cursor = clicks_col.find(
        {"targetId": contentId},
        {"_id": 0, "sourceId": 1, "count": 1}
    ).sort("count", -1)

    clicks = list(cursor)
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
    cursor = clicks_col.find(
        {"sourceId": contentId},
        {"_id": 0, "targetId": 1, "count": 1}
    ).sort("count", -1)

    clicks = list(cursor)
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
    # 1. Nodes in exact creation order (to determine default root)
    nodes_cursor = nodes_col.find(
        {},
        {"_id": 0, "contentId": 1, "name": 1, "description": 1, "status": 1, "createdAt": 1}
    ).sort("createdAt", ASCENDING)

    nodes_raw = list(nodes_cursor)

    if not nodes_raw:
        return jsonify([])

    # 2. Fetch ALL relationships ordered by creation time (earliest link wins)
    rels_cursor = relationships_col.find(
        {},
        {"_id": 0, "parentId": 1, "childId": 1, "createdAt": 1}
    ).sort("createdAt", ASCENDING)

    relationships_raw = [(r["parentId"], r["childId"]) for r in rels_cursor]

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
            "description": n.get("description", ""),
            "status": n.get("status", "New"),
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
    clicks_col.delete_many({})
    relationships_col.delete_many({})
    nodes_col.delete_many({})
    return jsonify({"message": "Reset done"})


# -------------------------
# START SERVER
# -------------------------
if __name__ == "__main__":
    app.run(port=5000, debug=False)