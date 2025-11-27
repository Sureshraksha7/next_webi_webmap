from flask import Flask, request, jsonify, g
from flask_cors import CORS
import os
import uuid
import psycopg2
from psycopg2.pool import ThreadedConnectionPool
from psycopg2.extras import RealDictCursor
from datetime import datetime
from dotenv import load_dotenv
import logging
from functools import wraps
from werkzeug.middleware.proxy_fix import ProxyFix

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

load_dotenv()

# -------------------------
# Configuration
# -------------------------
app = Flask(__name__)
app.wsgi_app = ProxyFix(app.wsgi_app, x_proto=1, x_host=1)
CORS(app)

# Database connection pool
db_pool = None

def init_db_pool():
    global db_pool
    if not db_pool:
        db_pool = ThreadedConnectionPool(
            minconn=2,
            maxconn=20,
            dsn=os.environ.get("POSTGRES_URI"),
            cursor_factory=RealDictCursor
        )
        logger.info("Initialized database connection pool")

def get_db_connection():
    if not db_pool:
        init_db_pool()
    return db_pool.getconn()

def return_db_connection(conn):
    if db_pool:
        db_pool.putconn(conn)

# Database connection decorator
def with_db_connection(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        conn = get_db_connection()
        try:
            result = f(conn, *args, **kwargs)
            return result
        except Exception as e:
            logger.error(f"Database error in {f.__name__}: {str(e)}", exc_info=True)
            return jsonify({"error": "Database error"}), 500
        finally:
            return_db_connection(conn)
    return decorated_function

# Request timing middleware
@app.before_request
def start_timer():
    g.start_time = datetime.now()

@app.after_request
def log_request(response):
    if 'start_time' in g:
        duration = (datetime.now() - g.start_time).total_seconds()
        logger.info(f"Request {request.method} {request.path} took {duration:.4f}s - {response.status_code}")
    return response

# -------------------------
# Helper Functions
# -------------------------
def now_utc():
    return datetime.utcnow()

def validate_node_data(data, require_id=False):
    if not isinstance(data, dict):
        return False, "Invalid data format"
    
    if 'name' not in data or not data['name'].strip():
        return False, "Name is required"
    
    if require_id and 'contentId' not in data:
        return False, "Content ID is required"
    
    return True, ""

# -------------------------
# API Endpoints
# -------------------------
@app.route("/node/create", methods=["POST"])
@with_db_connection
def create_node(conn):
    data = request.get_json(force=True)
    is_valid, message = validate_node_data(data)
    if not is_valid:
        return jsonify({"error": message}), 400

    content_id = str(uuid.uuid4())
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO nodes (content_id, name, description, status, created_at)
            VALUES (%s, %s, %s, %s, %s)
            RETURNING content_id, name, description, status
            """,
            (content_id, 
             data['name'].strip(), 
             data.get('description', '').strip(), 
             data.get('status', 'New'),
             now_utc())
        )
        result = cur.fetchone()
        conn.commit()
    
    return jsonify(dict(result))

@app.route("/node/update/<content_id>", methods=["PUT"])
@with_db_connection
def update_node(conn, content_id):
    data = request.get_json(force=True)
    is_valid, message = validate_node_data(data)
    if not is_valid:
        return jsonify({"error": message}), 400

    with conn.cursor() as cur:
        # Check if node exists
        cur.execute("SELECT 1 FROM nodes WHERE content_id = %s", (content_id,))
        if not cur.fetchone():
            return jsonify({"error": "Node not found"}), 404

        # Update node
        cur.execute(
            """
            UPDATE nodes
            SET name = %s,
                description = %s,
                status = %s
            WHERE content_id = %s
            RETURNING content_id, name, description, status
            """,
            (data['name'].strip(),
             data.get('description', '').strip(),
             data.get('status', 'New'),
             content_id)
        )
        result = cur.fetchone()
        conn.commit()
    
    return jsonify(dict(result))

@app.route("/node/delete/<content_id>", methods=["DELETE"])
@with_db_connection
def delete_node(conn, content_id):
    with conn.cursor() as cur:
        # Check if node exists
        cur.execute("SELECT 1 FROM nodes WHERE content_id = %s", (content_id,))
        if not cur.fetchone():
            return jsonify({"error": "Node not found"}), 404

        # Start transaction
        conn.set_session(autocommit=False)
        try:
            # Delete relationships
            cur.execute("DELETE FROM relationships WHERE parent_id = %s OR child_id = %s", 
                       (content_id, content_id))
            
            # Delete clicks
            cur.execute("DELETE FROM clicks WHERE source_id = %s OR target_id = %s", 
                       (content_id, content_id))
            
            # Delete node
            cur.execute("DELETE FROM nodes WHERE content_id = %s RETURNING content_id", 
                       (content_id,))
            
            if not cur.fetchone():
                conn.rollback()
                return jsonify({"error": "Node not found"}), 404
                
            conn.commit()
            return jsonify({"message": "Node deleted"})
            
        except Exception as e:
            conn.rollback()
            logger.error(f"Error deleting node {content_id}: {str(e)}")
            return jsonify({"error": "Failed to delete node"}), 500

@app.route("/node/search_unrelated/<content_id>/<search_term>", methods=["GET"])
@with_db_connection
def search_unrelated_nodes(conn, content_id, search_term):
    search_text = f"%{search_term.replace('_', ' ')}%"
    
    with conn.cursor() as cur:
        # Get all nodes matching search, excluding the parent itself
        cur.execute("""
            SELECT content_id AS "contentId", name, description, status
            FROM nodes
            WHERE content_id != %s
            AND (name ILIKE %s OR description ILIKE %s)
            AND content_id NOT IN (
                SELECT child_id FROM relationships WHERE parent_id = %s
            )
        """, (content_id, search_text, search_text, content_id))
        
        results = cur.fetchall()
        
    if not results:
        return jsonify({"message": "No unrelated match"}), 404
    
    return jsonify(results)

@app.route("/node/search/<search_term>", methods=["GET"])
@with_db_connection
def search_nodes(conn, search_term):
    search_text = f"%{search_term.replace('_', ' ')}%"
    
    with conn.cursor() as cur:
        cur.execute("""
            SELECT content_id AS "contentId", name, description, status
            FROM nodes
            WHERE name ILIKE %s OR description ILIKE %s
        """, (search_text, search_text))
        
        results = cur.fetchall()
    
    if not results:
        return jsonify({"message": "No match"}), 404
    
    return jsonify(results)

@app.route("/relation/create", methods=["POST"])
@with_db_connection
def create_relation(conn):
    data = request.get_json(force=True)
    parent_id = data.get("parentId")
    child_id = data.get("childId")
    
    if not parent_id or not child_id:
        return jsonify({"error": "parentId and childId are required"}), 400
    
    if parent_id == child_id:
        return jsonify({"error": "Cannot create self-relationship"}), 400
    
    with conn.cursor() as cur:
        # Check if nodes exist
        cur.execute("SELECT 1 FROM nodes WHERE content_id IN (%s, %s)", (parent_id, child_id))
        if len(cur.fetchall()) != 2:
            return jsonify({"error": "One or both nodes not found"}), 404
        
        # Check if relationship already exists
        cur.execute("""
            INSERT INTO relationships (parent_id, child_id, created_at)
            VALUES (%s, %s, %s)
            ON CONFLICT (parent_id, child_id) DO NOTHING
            RETURNING id
        """, (parent_id, child_id, now_utc()))
        
        if cur.rowcount == 0:
            return jsonify({"message": "Relationship already exists"}), 200
            
        conn.commit()
        return jsonify({"message": "Relationship created"})

@app.route("/relation/delete", methods=["DELETE"])
@with_db_connection
def delete_relation(conn):
    data = request.get_json(force=True)
    parent_id = data.get("parentId")
    child_id = data.get("childId")
    
    if not parent_id or not child_id:
        return jsonify({"error": "parentId and childId are required"}), 400
    
    with conn.cursor() as cur:
        # Delete clicks first
        cur.execute("""
            DELETE FROM clicks 
            WHERE (source_id = %s AND target_id = %s)
            OR (source_id = %s AND target_id = %s)
        """, (parent_id, child_id, child_id, parent_id))
        
        # Delete relationship
        cur.execute("""
            DELETE FROM relationships 
            WHERE parent_id = %s AND child_id = %s
            RETURNING id
        """, (parent_id, child_id))
        
        if cur.rowcount == 0:
            return jsonify({"error": "Relationship not found"}), 404
            
        conn.commit()
        return jsonify({"message": "Relationship deleted"})

@app.route("/link/click", methods=["POST"])
@with_db_connection
def click_link(conn):
    data = request.get_json(force=True)
    source_id = data.get("sourceId")
    target_id = data.get("targetId")
    
    if not source_id or not target_id:
        return jsonify({"error": "sourceId and targetId are required"}), 400
    
    now = now_utc()
    
    with conn.cursor() as cur:
        # Check if nodes exist
        cur.execute("SELECT 1 FROM nodes WHERE content_id IN (%s, %s)", (source_id, target_id))
        if len(cur.fetchall()) != 2:
            return jsonify({"error": "One or both nodes not found"}), 404
        
        # Update or insert click
        cur.execute("""
            INSERT INTO clicks (source_id, target_id, count, first_clicked, last_clicked)
            VALUES (%s, %s, 1, %s, %s)
            ON CONFLICT (source_id, target_id) 
            DO UPDATE SET 
                count = clicks.count + 1,
                last_clicked = EXCLUDED.last_clicked
            RETURNING count
        """, (source_id, target_id, now, now))
        
        conn.commit()
        return jsonify({"message": "Click recorded", "count": cur.fetchone()['count']})

@app.route("/inbound_stats/<content_id>", methods=["GET"])
@with_db_connection
def inbound_stats(conn, content_id):
    with conn.cursor() as cur:
        # Get total inbound clicks
        cur.execute("""
            SELECT COALESCE(SUM(count), 0) AS total_inbound_count
            FROM clicks
            WHERE target_id = %s
        """, (content_id,))
        total = cur.fetchone()['total_inbound_count']
        
        # Get detailed inbound connections
        cur.execute("""
            SELECT source_id AS "sourceId", count
            FROM clicks
            WHERE target_id = %s
            ORDER BY count DESC
        """, (content_id,))
        
        connections = cur.fetchall()
    
    return jsonify({
        "total_inbound_count": total,
        "inbound_connections": connections
    })

@app.route("/outbound_stats/<content_id>", methods=["GET"])
@with_db_connection
def outbound_stats(conn, content_id):
    with conn.cursor() as cur:
        # Get total outbound clicks
        cur.execute("""
            SELECT COALESCE(SUM(count), 0) AS total_outbound_count
            FROM clicks
            WHERE source_id = %s
        """, (content_id,))
        total = cur.fetchone()['total_outbound_count']
        
        # Get detailed outbound connections
        cur.execute("""
            SELECT target_id AS "targetId", count
            FROM clicks
            WHERE source_id = %s
            ORDER BY count DESC
        """, (content_id,))
        
        connections = cur.fetchall()
    
    return jsonify({
        "total_outbound_count": total,
        "outbound_connections": connections
    })

@app.route("/tree", methods=["GET"])
@with_db_connection
def get_tree(conn):
    with conn.cursor() as cur:
        # Fetch all nodes with required fields only
        cur.execute("""
            SELECT 
                content_id AS "contentId",
                name,
                description,
                status
            FROM nodes
            ORDER BY created_at
        """)
        nodes = {row['contentId']: dict(row) for row in cur.fetchall()}
        
        if not nodes:
            return jsonify([])
            
        # Fetch all relationships
        cur.execute("""
            SELECT parent_id, child_id 
            FROM relationships
            ORDER BY created_at
        """)
        
        # Build tree structure
        tree = []
        node_map = {node_id: {'contentId': node_id, **data, 'children': []} 
                   for node_id, data in nodes.items()}
        
        # Track all child nodes to find root
        child_nodes = set()
        
        for rel in cur:
            parent_id = rel['parent_id']
            child_id = rel['child_id']
            if parent_id in node_map and child_id in node_map:
                node_map[parent_id]['children'].append(child_id)
                child_nodes.add(child_id)
        
        # Find root nodes (nodes that are not children of any other node)
        root_nodes = [node_id for node_id in node_map if node_id not in child_nodes]
        
        # If no root found (shouldn't happen in a valid tree), use first node
        if not root_nodes and node_map:
            root_nodes = [next(iter(node_map))]
        
        # Convert to list of root nodes with their subtrees
        result = [node_map[node_id] for node_id in root_nodes]
        
        return jsonify(result)

@app.route("/stats/all", methods=["GET"])
@with_db_connection
def get_all_stats(conn):
    with conn.cursor() as cur:
        # Get total inbound clicks per node
        cur.execute("""
            SELECT 
                target_id AS node_id, 
                COALESCE(SUM(count), 0) AS total_inbound_count
            FROM clicks
            GROUP BY target_id
        """)
        inbound_stats = {row['node_id']: row['total_inbound_count'] for row in cur.fetchall()}
        
        # Get total outbound clicks per node
        cur.execute("""
            SELECT 
                source_id AS node_id, 
                COALESCE(SUM(count), 0) AS total_outbound_count
            FROM clicks
            GROUP BY source_id
        """)
        outbound_stats = {row['node_id']: row['total_outbound_count'] for row in cur.fetchall()}
        
        # Get all node IDs
        cur.execute("SELECT content_id FROM nodes")
        all_node_ids = {row['content_id'] for row in cur.fetchall()}
        
        # Combine results with 0 as default for nodes with no clicks
        result = {
            node_id: {
                "total_inbound_count": inbound_stats.get(node_id, 0),
                "total_outbound_count": outbound_stats.get(node_id, 0)
            }
            for node_id in all_node_ids
        }
        
        return jsonify(result)

@app.route("/reset", methods=["DELETE"])
@with_db_connection
def reset_all(conn):
    with conn.cursor() as cur:
        cur.execute("TRUNCATE TABLE clicks, relationships, nodes RESTART IDENTITY CASCADE")
        conn.commit()
    return jsonify({"message": "All data has been reset"})

@app.route("/health", methods=["GET"])
def health_check():
    return jsonify({"status": "healthy"})

# Initialize connection pool on startup
@app.before_first_request
def startup():
    init_db_pool()

# Clean up on shutdown
def shutdown():
    if db_pool:
        db_pool.closeall()

if __name__ == "__main__":
    try:
        port = int(os.environ.get("PORT", 5000))
        app.run(host="0.0.0.0", port=port, threaded=True)
    finally:
        shutdown()