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
from flask_caching import Cache
import time
from psycopg2 import OperationalError
from typing import Set, Dict, Any, List
import json

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

load_dotenv()

# Configuration
app = Flask(__name__)
app.wsgi_app = ProxyFix(app.wsgi_app, x_proto=1, x_host=1)
CORS(app)

# Cache configuration
cache = Cache(config={
    'CACHE_TYPE': 'redis',
    'CACHE_REDIS_URL': os.environ.get('REDIS_URL', 'redis://localhost:6379/0'),
    'CACHE_DEFAULT_TIMEOUT': 60,  # 1 minute
    'CACHE_KEY_PREFIX': 'flow_'
})
cache.init_app(app)
#hiiii
# Constants
DB_CONNECTION_TIMEOUT = 5  # seconds
QUERY_TIMEOUT = 10  # seconds

# Database connection pool
db_pool = None

def init_db_pool():
    global db_pool
    if not db_pool:
        db_pool = ThreadedConnectionPool(
            minconn=5,
            maxconn=30,
            dsn=os.environ.get("POSTGRES_URI"),
            cursor_factory=RealDictCursor,
            keepalives=1,
            keepalives_idle=30,
            keepalives_interval=10,
            keepalives_count=5,
            connect_timeout=DB_CONNECTION_TIMEOUT
        )
        logger.info("Initialized database connection pool")

def get_db_connection():
    if not db_pool:
        init_db_pool()
    return db_pool.getconn()

def return_db_connection(conn):
    if db_pool:
        db_pool.putconn(conn)

# Database connection decorator with retry logic
def with_db_connection(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        max_retries = 3
        retry_delay = 0.5  # seconds
        
        for attempt in range(max_retries):
            conn = get_db_connection()
            try:
                with conn.cursor() as cur:
                    cur.execute(f"SET statement_timeout = {QUERY_TIMEOUT * 1000}")
                result = f(conn, *args, **kwargs)
                return result
            except (OperationalError, psycopg2.OperationalError) as e:
                logger.warning(f"Database operation failed (attempt {attempt + 1}/{max_retries}): {str(e)}")
                if attempt == max_retries - 1:
                    logger.error("Max retries reached")
                    return jsonify({"error": "Database operation timed out"}), 503
                time.sleep(retry_delay * (attempt + 1))
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

# Helper Functions
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

# Cache key generator
def make_cache_key(*args, **kwargs):
    path = request.path
    args = request.args
    args_str = str(hash(frozenset(args.items())))
    return f"{path}:{args_str}"

# Cache invalidation
def invalidate_cache(*keys):
    for key in keys:
        cache.delete(key)

# Create indexes on startup
def create_indexes(conn):
    with conn.cursor() as cur:
        cur.execute("""
            DO $$
            BEGIN
                -- Nodes table indexes
                IF NOT EXISTS (
                    SELECT 1 FROM pg_indexes 
                    WHERE tablename = 'nodes' AND indexname = 'idx_nodes_content_id'
                ) THEN
                    CREATE INDEX idx_nodes_content_id ON nodes(content_id);
                END IF;
                
                -- Relationships table indexes
                IF NOT EXISTS (
                    SELECT 1 FROM pg_indexes 
                    WHERE tablename = 'relationships' AND indexname = 'idx_relationships_parent_child'
                ) THEN
                    CREATE INDEX idx_relationships_parent_child ON relationships(parent_id, child_id);
                END IF;
                
                IF NOT EXISTS (
                    SELECT 1 FROM pg_indexes 
                    WHERE tablename = 'relationships' AND indexname = 'idx_relationships_child'
                ) THEN
                    CREATE INDEX idx_relationships_child ON relationships(child_id);
                END IF;
                
                -- Clicks table indexes
                IF NOT EXISTS (
                    SELECT 1 FROM pg_indexes 
                    WHERE tablename = 'clicks' AND indexname = 'idx_clicks_source_target'
                ) THEN
                    CREATE INDEX idx_clicks_source_target ON clicks(source_id, target_id);
                END IF;
                
                IF NOT EXISTS (
                    SELECT 1 FROM pg_indexes 
                    WHERE tablename = 'clicks' AND indexname = 'idx_clicks_target'
                ) THEN
                    CREATE INDEX idx_clicks_target ON clicks(target_id);
                END IF;
            END
            $$;
        """)
        conn.commit()
@app.route('/tree', methods=['GET'])
@with_db_connection
@cache.memoize(timeout=60)
def get_tree(conn):
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            # Fetch all nodes
            cur.execute("""
                SELECT content_id, title, parent_id, content, 
                       created_at, updated_at, level, position, 
                       is_expanded, is_leaf, is_loading, is_editing, 
                       is_renaming, is_selected, is_hidden, has_children, 
                       children, metadata, type, icon, is_checked, 
                       is_indeterminate, is_draggable, is_droppable, 
                       is_selectable, is_dragging, is_drop_target, 
                       is_drop_allowed, is_drop_disabled, is_drop_ancestor
                FROM nodes
                ORDER BY position
            """)
            nodes = cur.fetchall()
            
            # Convert UUID and datetime to string for JSON serialization
            for node in nodes:
                if 'content_id' in node:
                    node['content_id'] = str(node['content_id'])
                if 'created_at' in node and node['created_at']:
                    node['created_at'] = node['created_at'].isoformat()
                if 'updated_at' in node and node['updated_at']:
                    node['updated_at'] = node['updated_at'].isoformat()
                    
            return jsonify(nodes)
            
    except Exception as e:
        logger.error(f"Error fetching tree: {str(e)}")
        return jsonify({"error": "Failed to fetch tree data"}), 500
# API Endpoints
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
    
    # Invalidate cache
    cache.delete_memoized(get_tree)
    cache.delete('all_stats')
    
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
    
    # Invalidate cache
    cache.delete_memoized(get_tree)
    cache.delete('all_stats')
    
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
            
            # Invalidate cache
            cache.delete_memoized(get_tree)
            cache.delete('all_stats')
            
            return jsonify({"message": "Node deleted"})
            
        except Exception as e:
            conn.rollback()
            logger.error(f"Error deleting node {content_id}: {str(e)}")
            return jsonify({"error": "Failed to delete node"}), 500

# [Previous endpoints for search, relationships, clicks, stats, etc. remain exactly the same]
# ... (include all your existing endpoints here without modification)

# Initialize connection pool and create indexes on startup
def startup():
    init_db_pool()
    # Create database indexes on startup
    conn = get_db_connection()
    try:
        create_indexes(conn)
    except Exception as e:
        logger.error(f"Failed to create database indexes: {str(e)}")
    finally:
        return_db_connection(conn)

# Call the function when the app starts
with app.app_context():
    startup()

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