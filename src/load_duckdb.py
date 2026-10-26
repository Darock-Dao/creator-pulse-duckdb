import os
import duckdb

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
DB_PATH = os.path.join(PROJECT_ROOT, "data", "creator_pulse.duckdb")
DEFAULT_STAGED_FILE = os.path.join(PROJECT_ROOT, "data", "staged", "snapshots.jsonl")
DEFAULT_WATCHLIST_BACKUP = os.path.join(PROJECT_ROOT, "data", "snowflake_backup", "watchlist.csv")

def get_duckdb_connection(db_path=DB_PATH, read_only=False):
    """Establishes and returns a connection to the local DuckDB database."""
    db_dir = os.path.dirname(db_path)
    if db_dir:
        os.makedirs(db_dir, exist_ok=True)
    return duckdb.connect(db_path, read_only=read_only)

def create_raw_tables(conn):
    """Creates the raw_video_snapshots and watchlist tables if they do not exist."""
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS raw_video_snapshots (
            snapshot_id VARCHAR,
            video_id VARCHAR,
            channel_id VARCHAR,
            channel_title VARCHAR,
            video_title VARCHAR,
            published_at TIMESTAMP,
            view_count BIGINT,
            like_count BIGINT,
            comment_count BIGINT,
            extracted_at TIMESTAMP,
            loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );

        CREATE TABLE IF NOT EXISTS watchlist (
            channel_id VARCHAR,
            channel_handle VARCHAR,
            channel_title VARCHAR,
            thumbnail_url VARCHAR,
            added_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            is_active BOOLEAN DEFAULT TRUE
        );

        CREATE UNIQUE INDEX IF NOT EXISTS idx_watchlist_channel_id ON watchlist (channel_id);
        """
    )

    # Auto-seed watchlist from backup if table is empty
    count = conn.execute("SELECT COUNT(*) FROM watchlist;").fetchone()[0]
    if count == 0 and os.path.exists(DEFAULT_WATCHLIST_BACKUP):
        conn.execute(
            f"""
            INSERT INTO watchlist BY NAME
            SELECT 
                CHANNEL_ID AS channel_id,
                CHANNEL_HANDLE AS channel_handle,
                CHANNEL_TITLE AS channel_title,
                THUMBNAIL_URL AS thumbnail_url,
                ADDED_AT::TIMESTAMP AS added_at,
                IS_ACTIVE::BOOLEAN AS is_active
            FROM read_csv_auto('{DEFAULT_WATCHLIST_BACKUP}');
            """
        )


def load_snapshots(conn, file_path=DEFAULT_STAGED_FILE):
    """Loads staged JSONL data directly into raw_video_snapshots."""
    conn.execute(
        f"""INSERT INTO raw_video_snapshots (
        snapshot_id, video_id, channel_id, channel_title, video_title,
        published_at, view_count, like_count, comment_count, extracted_at, loaded_at
        )
        SELECT 
            snapshot_id,
            video_id,
            channel_id,
            channel_title,
            video_title,
            published_at::TIMESTAMP,
            view_count::BIGINT,
            like_count::BIGINT,
            comment_count::BIGINT,
            extracted_at::TIMESTAMP,
            CURRENT_TIMESTAMP AS loaded_at
        FROM read_json_auto('{file_path}')"""
    )

def run_load_pipeline(source_file=DEFAULT_STAGED_FILE):
    """Main entrypoint called by Airflow and Streamlit."""
    if not os.path.exists(source_file):
        raise FileNotFoundError(f"Staged file not found: {source_file}")

    conn = get_duckdb_connection()
    try:
        create_raw_tables(conn)
        load_snapshots(conn, source_file)
        print(f"Successfully loaded {source_file} into raw_video_snapshots.")
    finally:
        conn.close()

