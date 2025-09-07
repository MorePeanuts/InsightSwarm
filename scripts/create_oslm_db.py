# /// script
# requires-python = ">=3.12"
# dependencies = []
# ///
import sqlite3

DB_FILE = "database/oslm_tmp.db"

MODEL_DIM_HF_SQL = """
CREATE TABLE IF NOT EXISTS model_hf (
    repo                  TEXT NOT NULL,
    model_name            TEXT NOT NULL,
    modality              TEXT NOT NULL CHECK(modality IN ('语言', '语音', '视觉', '多模态', '向量', '蛋白质', '3D', '具身')),
    org                   TEXT NOT NULL,
    downloads_last_month  INTEGER,
    likes                 INTEGER,
    descendants           INTEGER,
    community             INTEGER,
    param_size            INTEGER,
    date_begin            TEXT,
    date_end              TEXT,
    hf_link               TEXT,
    img_path              TEXT,
    PRIMARY KEY (repo, model_name, date_end)
);
"""

MODEL_DIM_MS_SQL = """
CREATE TABLE IF NOT EXISTS model_ms (
    repo              TEXT NOT NULL,
    model_name        TEXT NOT NULL,
    modality          TEXT NOT NULL CHECK(modality IN ('语言', '语音', '视觉', '多模态', '向量', '蛋白质', '3D', '具身')),
    org               TEXT NOT NULL,
    downloads_last_month  INTEGER,
    downloads_total   INTEGER,
    likes             INTEGER,
    community         INTEGER,
    param_size        INTEGER,
    date_begin        TEXT,
    date_end          TEXT,
    ms_link           TEXT,
    img_path          TEXT,
    PRIMARY KEY (repo, model_name, date_end)
);
"""


def create_oslm_tables(sql: str):
    """
    Connect to the SQLite database and create or update the table structure using a composite primary key.
    """
    try:
        conn = sqlite3.connect(DB_FILE)
        cursor = conn.cursor()
        print(f"Successfully connected to the database: {DB_FILE}")

        # If you need to rebuild the table completely, you can first execute the deletion operation
        # cursor.execute(f"DROP TABLE IF EXISTS {TABLE_NAME};")
        # print(f"Old table '{TABLE_NAME}' (if it exists) has been deleted.")
    
        # --- Execute the SQL statement ---
        cursor.execute(sql)
        print("Table has been successfully created or already exists.")

        conn.commit()

    except sqlite3.Error as e:
        print(f"Database operation error: {e}")
    finally:
        if conn:
            conn.close()
            print("Database connection has been closed.")

if __name__ == "__main__":
    create_oslm_tables(MODEL_DIM_HF_SQL)
    create_oslm_tables(MODEL_DIM_MS_SQL)
