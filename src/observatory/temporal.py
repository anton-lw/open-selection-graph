"""Point-in-time truth over append-only normalized OSG tables."""

from __future__ import annotations

from datetime import datetime
from typing import Iterable

from .schema import PRIMARY_KEYS


def as_of_sql(table: str, cutoff: str, *, include_deleted: bool = False) -> str:
    if table not in PRIMARY_KEYS:
        raise KeyError(table)
    datetime.fromisoformat(cutoff.replace("Z", "+00:00"))
    key = PRIMARY_KEYS[table]
    safe_cutoff = cutoff.replace("'", "''")
    deleted = "" if include_deleted or table != "source_object" else (
        f" AND (deleted_at IS NULL OR deleted_at > TIMESTAMPTZ '{safe_cutoff}')"
    )
    return f"""
        SELECT * EXCLUDE (_rn) FROM (
          SELECT *, row_number() OVER (
            PARTITION BY \"{key}\" ORDER BY observed_at DESC, record_version DESC
          ) AS _rn
          FROM \"{table}\"
          WHERE observed_at <= TIMESTAMPTZ '{safe_cutoff}'{deleted}
        ) WHERE _rn = 1
    """ if table != "source_object" else f"""
        SELECT * EXCLUDE (_rn) FROM (
          SELECT *, row_number() OVER (
            PARTITION BY source_id, native_id ORDER BY retrieved_at DESC
          ) AS _rn
          FROM source_object
          WHERE retrieved_at <= TIMESTAMPTZ '{safe_cutoff}'
        ) WHERE _rn = 1{deleted}
    """


def install_as_of_views(connection, cutoff: str, tables: Iterable[str] | None = None) -> None:
    for table in tables or PRIMARY_KEYS:
        existing = {row[0] for row in connection.execute("SHOW TABLES").fetchall()}
        if table in existing:
            connection.execute(f'CREATE OR REPLACE VIEW "asof_{table}" AS {as_of_sql(table, cutoff)}')
