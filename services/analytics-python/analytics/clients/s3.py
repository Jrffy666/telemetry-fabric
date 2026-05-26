"""S3/Parquet readers for offline analytics."""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from typing import Any, Iterable, Mapping, Sequence

from analytics.exceptions import OptionalDependencyError


@dataclass(frozen=True)
class S3ParquetConfig:
    endpoint_url: str | None = None
    region: str | None = None
    access_key_id: str | None = None
    secret_access_key: str | None = None
    session_token: str | None = None
    default_row_limit: int = 100_000
    batch_size: int = 10_000
    storage_options: Mapping[str, Any] = field(default_factory=dict)

    @classmethod
    def from_env(cls, prefix: str = "S3_") -> "S3ParquetConfig":
        return cls(
            endpoint_url=os.getenv(f"{prefix}ENDPOINT_URL"),
            region=os.getenv(f"{prefix}REGION"),
            access_key_id=os.getenv(f"{prefix}ACCESS_KEY_ID"),
            secret_access_key=os.getenv(f"{prefix}SECRET_ACCESS_KEY"),
            session_token=os.getenv(f"{prefix}SESSION_TOKEN"),
            default_row_limit=int(
                os.getenv(f"{prefix}DEFAULT_ROW_LIMIT", str(cls.default_row_limit))
            ),
            batch_size=int(os.getenv(f"{prefix}BATCH_SIZE", str(cls.batch_size))),
        )

    @classmethod
    def from_mapping(cls, values: Mapping[str, Any] | None) -> "S3ParquetConfig":
        values = values or {}
        return cls(
            endpoint_url=values.get("endpoint_url"),
            region=values.get("region"),
            access_key_id=values.get("access_key_id"),
            secret_access_key=values.get("secret_access_key"),
            session_token=values.get("session_token"),
            default_row_limit=int(values.get("default_row_limit", cls.default_row_limit)),
            batch_size=int(values.get("batch_size", cls.batch_size)),
            storage_options=dict(values.get("storage_options", {})),
        )


class S3ParquetReader:
    """Read Parquet datasets from S3-compatible object storage."""

    def __init__(self, config: S3ParquetConfig | None = None) -> None:
        self.config = config or S3ParquetConfig.from_env()

    def read_polars(
        self,
        path: str,
        columns: Sequence[str] | None = None,
        *,
        row_limit: int | None = None,
    ) -> Any:
        lazy_frame = self.scan_polars(path, columns=columns)
        limit = row_limit if row_limit is not None else self.config.default_row_limit
        return lazy_frame.limit(limit).collect(streaming=True)

    def scan_polars(self, path: str, columns: Sequence[str] | None = None) -> Any:
        try:
            import polars as pl
        except ModuleNotFoundError as exc:
            raise OptionalDependencyError("polars", "lazy Polars Parquet scans") from exc

        scan = pl.scan_parquet(path, storage_options=self._storage_options())
        if columns:
            scan = scan.select(list(columns))
        return scan

    def read_pandas(
        self,
        path: str,
        columns: Sequence[str] | None = None,
        *,
        row_limit: int | None = None,
    ) -> Any:
        try:
            import pandas as pd
        except ModuleNotFoundError as exc:
            raise OptionalDependencyError("pandas", "Pandas Parquet reads") from exc

        frames = []
        remaining = row_limit if row_limit is not None else self.config.default_row_limit
        for batch in self.iter_pyarrow_batches(path, columns=columns, batch_size=self.config.batch_size):
            if remaining <= 0:
                break
            table = batch.to_table()
            if table.num_rows > remaining:
                table = table.slice(0, remaining)
            frames.append(table.to_pandas())
            remaining -= table.num_rows
        if not frames:
            return pd.DataFrame()
        return pd.concat(frames, ignore_index=True)

    def iter_pyarrow_batches(
        self,
        path: str,
        columns: Sequence[str] | None = None,
        *,
        batch_size: int | None = None,
    ) -> Iterable[Any]:
        try:
            import pyarrow.dataset as ds
            import pyarrow.fs as fs
        except ModuleNotFoundError as exc:
            raise OptionalDependencyError("pyarrow", "streaming Parquet reads") from exc

        filesystem = self._pyarrow_filesystem(fs)
        dataset = ds.dataset(path, filesystem=filesystem, format="parquet")
        scanner = dataset.scanner(
            columns=list(columns) if columns else None,
            batch_size=batch_size or self.config.batch_size,
        )
        yield from scanner.to_batches()

    def query_duckdb(self, path: str, sql: str) -> Any:
        try:
            import duckdb
        except ModuleNotFoundError as exc:
            raise OptionalDependencyError("duckdb", "DuckDB Parquet queries") from exc

        connection = duckdb.connect(database=":memory:")
        try:
            connection.execute("INSTALL httpfs; LOAD httpfs;")
            self._configure_duckdb_s3(connection)
            connection.execute("CREATE VIEW parquet_input AS SELECT * FROM read_parquet(?)", [path])
            return connection.execute(sql).fetch_df()
        finally:
            connection.close()

    def _storage_options(self) -> dict[str, Any]:
        options = dict(self.config.storage_options)
        if self.config.endpoint_url:
            options["endpoint_url"] = self.config.endpoint_url
        if self.config.region:
            options["region"] = self.config.region
        if self.config.access_key_id:
            options["aws_access_key_id"] = self.config.access_key_id
        if self.config.secret_access_key:
            options["aws_secret_access_key"] = self.config.secret_access_key
        if self.config.session_token:
            options["aws_session_token"] = self.config.session_token
        return options

    def _pyarrow_filesystem(self, fs_module: Any) -> Any:
        if self.config.endpoint_url or self.config.region or self.config.access_key_id:
            return fs_module.S3FileSystem(
                access_key=self.config.access_key_id,
                secret_key=self.config.secret_access_key,
                session_token=self.config.session_token,
                region=self.config.region,
                endpoint_override=self.config.endpoint_url,
            )
        return None

    def _configure_duckdb_s3(self, connection: Any) -> None:
        if self.config.region:
            connection.execute("SET s3_region=?", [self.config.region])
        if self.config.endpoint_url:
            connection.execute("SET s3_endpoint=?", [self.config.endpoint_url])
        if self.config.access_key_id:
            connection.execute("SET s3_access_key_id=?", [self.config.access_key_id])
        if self.config.secret_access_key:
            connection.execute("SET s3_secret_access_key=?", [self.config.secret_access_key])
        if self.config.session_token:
            connection.execute("SET s3_session_token=?", [self.config.session_token])
