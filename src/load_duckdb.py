import os
import duckdb

DB_PATH = "data/creator_pulse.duckdb"

def get_duckdb_connection(db_path=DB_PATH):
    """Establishes and returns a connection to the local DuckDB database."""
    return duckdb.connect(db_path)

def create_raw_tables(conn):
    """Creates the raw_video_snapshots table if it doesn't already exist."""
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
        """
    )


def load_snapshots(conn, file_path="data/staged/snapshots.jsonl"):
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
    FROM read_json_auto('{file_path}')""")

def run_load_pipeline(source_file="data/staged/snapshots.jsonl"):
    """Main entrypoint called by Airflow and Streamlit."""
    # 1. Check if source_file exists
    # 2. Get connection
    # 3. create_raw_tables(conn)
    # 4. load_snapshots(conn, source_file)
    # 5. Close connection

    if not os.path.exists(source_file):
        raise FileNotFoundError(f"Staged file not found: {source_file}")

    conn = get_duckdb_connection()
    try:
        create_raw_tables(conn)
        load_snapshots(conn, source_file)
        print(f"Successfully loaded {source_file} into raw_video_snapshots.")
    finally:
        conn.close()

