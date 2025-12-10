# queries/loader.py
from pathlib import Path


def load_sql_query(query_path: str) -> str:
    """Load a SQL query from file."""
    base_dir = Path(__file__).parent / "sql"
    query_file = base_dir / f"{query_path}.sql"

    if not query_file.exists():
        raise FileNotFoundError(f"SQL query file not found: {query_file}")

    with open(query_file, "r") as f:
        return f.read()