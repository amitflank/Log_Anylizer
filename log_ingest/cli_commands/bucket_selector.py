"""Bucket selection functionality.

This module handles selecting buckets from a DataFrame for various purposes.
"""

import json
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, ClassVar, Optional, Union

import deal
import duckdb
import pyarrow as pa
import pyarrow.parquet as pq

from log_ingest.bucket_building.bucket_builder import TimeBucket


class BucketSelector:
    """Handles selection of buckets from a parquet file for various display/analysis purposes."""

    # SQL query templates for various operations
    _QUERIES: ClassVar[dict[str, str]] = {
        "random": "SELECT * FROM '{path}' USING SAMPLE {n} ROWS",
        "top_n": "SELECT * FROM '{path}' ORDER BY {field} DESC LIMIT {n}",
        "bottom_n": "SELECT * FROM '{path}' ORDER BY {field} ASC LIMIT {n}",
        "count": "SELECT COUNT(*) as count FROM '{path}'",
        "time_range": "SELECT MIN(start_time) as min_start, MAX(end_time) as max_end FROM '{path}'",
        "by_flags": "SELECT * FROM '{path}' WHERE {conditions}",
        "by_thresholds": "SELECT * FROM '{path}' WHERE {conditions}",
        "by_time_range": "SELECT * FROM '{path}' WHERE start_time < {end} AND end_time > {start}",
    }

    def __init__(self, parquet_path: Path):
        """Initialize with parquet file path.

        Args:
            parquet_path: Path to bucket parquet file

        Raises:
            ValueError: If parquet_path is None or doesn't exist
        """
        if parquet_path is None:
            raise ValueError("parquet_path cannot be None")

        if not parquet_path.exists():
            raise ValueError(f"Parquet file not found: {parquet_path}")

        self._parquet_path = parquet_path

    def _execute_query(
        self, query_key: str, return_type: str = "buckets", **kwargs
    ) -> list[TimeBucket] | int | tuple | None:
        """Execute a SQL query from the template dictionary.

        Args:
            query_key: Key in _QUERIES dictionary
            return_type: 'buckets', 'scalar', or 'row'
            **kwargs: Parameters to format the query template

        Returns:
            Depending on return_type:
            - 'buckets': List[TimeBucket]
            - 'dataframe': pd.DataFrame
            - 'scalar': single value
            - 'row': tuple of values
        """
        # Add path to kwargs
        kwargs["path"] = self._parquet_path

        # Get and format query
        query_template = self._QUERIES[query_key]
        query = query_template.format(**kwargs)

        # Execute query
        with duckdb.connect(":memory:") as conn:
            result = conn.execute(query)

            if return_type == "buckets":
                arrow_table = result.fetch_arrow_table()
                return self._arrow_to_timebuckets(arrow_table)
            if return_type == "scalar":
                row = result.fetchone()
                return row[0] if row else None
            if return_type == "row":
                return result.fetchone()
            raise ValueError(f"Unknown return_type: {return_type}")

    def is_empty(self) -> bool:
        """Check if selector has any buckets.

        Returns:
            True if no buckets available, False otherwise
        """
        pf = pq.ParquetFile(self._parquet_path)
        return pf.metadata.num_rows == 0

    def get_random(self, n: int = 1) -> list[TimeBucket]:
        """Get n random buckets for sanity checking.

        Args:
            n: Number of random buckets to select

        Returns:
            List of TimeBucket objects randomly selected

        Raises:
            ValueError: If n < 1 or n > available buckets
        """
        if n < 1:
            raise ValueError("n must be at least 1")

        num_rows = self.count()
        if num_rows == 0:
            raise ValueError("No buckets available")

        if n > num_rows:
            raise ValueError(f"Cannot select {n} buckets from {num_rows} available")

        return self._execute_query("random", n=n)

    def by_flags(self, limit: Optional[int] = None, **kwargs) -> list[TimeBucket]:
        """Filter buckets by boolean flag values.

        Args:
            limit: Optional maximum number of results to return
            **kwargs: Boolean field filters (e.g., has_spike=True, rebuild_active=False)

        Returns:
            List of TimeBucket objects matching the criteria

        Raises:
            ValueError: If no criteria provided

        Examples:
            selector.by_flags(has_spike=True)
            selector.by_flags(has_spike=True, rebuild_active=False, limit=100)
        """
        if not kwargs:
            raise ValueError("Must provide some filter criteria when searching by filter")

        # Build SQL WHERE conditions
        conditions = []
        for field_name, expected_value in kwargs.items():
            # Convert Python boolean to SQL boolean
            sql_value = (
                str(expected_value).upper() if isinstance(expected_value, bool) else expected_value
            )
            conditions.append(f"{field_name} = {sql_value}")

        conditions_str = " AND ".join(conditions)
        if limit:
            conditions_str += f" LIMIT {limit}"
        return self._execute_query("by_flags", conditions=conditions_str)

    def by_thresholds(self, limit: Optional[int] = None, **kwargs) -> list[TimeBucket]:
        """Filter buckets by numeric threshold comparisons.

        Args:
            limit: Optional maximum number of results to return
            **kwargs: Field comparisons using Click CLI format:
                     field_gt=value  (greater than)
                     field_gte=value (greater than or equal)
                     field_lt=value  (less than)
                     field_lte=value (less than or equal)
                     field_eq=value  (equal)
                     field_ne=value  (not equal)

        Returns:
            List of TimeBucket objects matching all criteria

        Raises:
            ValueError: If invalid format or operator

        Examples:
            selector.by_thresholds(write_latency_ms_gt=100)
            selector.by_thresholds(error_count_gte=5, cpu_util_lt=80, limit=100)
        """
        if not kwargs:
            raise ValueError("Must provide some threshold criteria")

        # Map operators to SQL operators
        op_map = {"gt": ">", "gte": ">=", "lt": "<", "lte": "<=", "eq": "=", "ne": "!="}

        # Build SQL WHERE conditions
        conditions = []
        for key, value in kwargs.items():
            # Split on last underscore to separate field from operator
            # e.g., "write_latency_ms_gt" -> ["write_latency_ms", "gt"]
            parts = key.rsplit("_", 1)
            expected_parts = 2  # field_name and operator
            if len(parts) != expected_parts:
                raise ValueError(f"Invalid threshold format: {key}. Use field_op=value")

            field_name, op = parts
            if op not in op_map:
                raise ValueError(f"Invalid operator: {op}. Use one of: {list(op_map.keys())}")

            conditions.append(f"{field_name} {op_map[op]} {value}")

        conditions_str = " AND ".join(conditions)
        if limit:
            conditions_str += f" LIMIT {limit}"
        return self._execute_query("by_thresholds", conditions=conditions_str)

    def by_time_range(self, start: datetime, end: datetime) -> list[TimeBucket]:
        """Filter buckets within a time range.

        Args:
            start: Start datetime (inclusive)
            end: End datetime (exclusive)

        Returns:
            List of TimeBucket objects within the time range

        Raises:
            ValueError: If start >= end

        Examples:
            from datetime import datetime, timezone
            start = datetime(2024, 1, 1, 10, 0, 0, tzinfo=timezone.utc)
            end = datetime(2024, 1, 1, 11, 0, 0, tzinfo=timezone.utc)
            selector.by_time_range(start, end)
        """
        if start >= end:
            raise ValueError("Start time must be before end time")

        # Format timestamps for SQL (DuckDB handles timezone-aware datetimes)
        return self._execute_query(
            "by_time_range", start=f"'{start.isoformat()}'", end=f"'{end.isoformat()}'"
        )

    def top_n(self, field: str, n: int = 5) -> list[TimeBucket]:
        """Get the top N buckets by a field value (descending order).

        Args:
            field: Field name to sort by
            n: Number of buckets to return (default: 5)

        Returns:
            List of top N TimeBucket objects sorted by field descending

        Raises:
            ValueError: If n < 1

        Examples:
            selector.top_n('total_events', 10)  # 10 busiest buckets
            selector.top_n('write_latency_ms', 5)  # 5 highest latency buckets
        """
        if n < 1:
            raise ValueError("n must be at least 1")

        return self._execute_query("top_n", field=field, n=n)

    def bottom_n(self, field: str, n: int = 5) -> list[TimeBucket]:
        """Get the bottom N buckets by a field value (ascending order).

        Args:
            field: Field name to sort by
            n: Number of buckets to return (default: 5)

        Returns:
            List of bottom N TimeBucket objects sorted by field ascending

        Raises:
            ValueError: If n < 1

        Examples:
            selector.bottom_n('total_events', 10)  # 10 least busy buckets
            selector.bottom_n('write_latency_ms', 5)  # 5 lowest latency buckets
        """
        if n < 1:
            raise ValueError("n must be at least 1")

        return self._execute_query("bottom_n", field=field, n=n)

    def count(self) -> int:
        """Get the total number of buckets in the parquet file.

        Returns:
            Number of buckets

        Examples:
            total = selector.count()
            print(f"Found {total} buckets")
        """
        result = self._execute_query("count", return_type="scalar")
        return result if result else 0

    def exists(self) -> bool:
        """Check if any buckets exist in the file.

        Returns:
            True if at least one bucket exists, False otherwise

        Examples:
            if selector.exists():
                buckets = selector.get_random(5)
        """
        return not self.is_empty()

    def get_time_range(self) -> Optional[tuple[datetime, datetime]]:
        """Get the time range covered by all buckets.

        Returns:
            Tuple of (earliest_start_time, latest_end_time) or None if no buckets

        Examples:
            time_range = selector.get_time_range()
            if time_range:
                start, end = time_range
                print(f"Data covers {start} to {end}")
        """
        if self.is_empty():
            return None

        result = self._execute_query("time_range", return_type="row")
        if result and result[0] and result[1]:
            return (result[0], result[1])
        return None

    def _arrow_to_timebuckets(self, arrow_table: pa.Table) -> list[TimeBucket]:
        """Convert PyArrow table to TimeBucket objects.

        Args:
            arrow_table: PyArrow table from DuckDB query

        Returns:
            List of TimeBucket objects
        """
        # Convert to Python dicts
        records = arrow_table.to_pylist()

        buckets = []
        for record in records:
            # Deserialize templates JSON back to dict
            templates_str = record.get("templates")
            if templates_str and isinstance(templates_str, str):
                try:
                    record["templates"] = json.loads(templates_str)
                except (json.JSONDecodeError, TypeError):
                    record["templates"] = {}
            else:
                record["templates"] = {}

            # Create TimeBucket directly
            bucket = TimeBucket(**record)
            buckets.append(bucket)

        return buckets

    # ------------------------------------------------------------------
    # Template analytics helpers
    # ------------------------------------------------------------------

    @deal.pre(lambda _, start=None, end=None, **__: start is None or end is None or start < end)
    @deal.post(lambda result: isinstance(result, list))
    @deal.post(lambda result: all(isinstance(r, dict) for r in result))
    def template_instances(
        self,
        template_ids: Optional[list[str]] = None,
        template_substring: Optional[str] = None,
        start: Optional[datetime] = None,
        end: Optional[datetime] = None,
        bucket_filters: Optional[dict[str, Any]] = None,
        var_filters: Optional[dict[str, Any]] = None,
        include_vars: bool = False,
    ) -> list[dict[str, Any]]:
        """Return flattened template instances matching the provided filters.

        Args:
            template_ids: Optional iterable of template IDs to include.
            template_substring: Optional substring (case-insensitive) that must
                appear in the template string.
            start: Optional inclusive lower bound for bucket overlap window.
            end: Optional exclusive upper bound for bucket overlap window.
            bucket_filters: Optional dict of bucket-level equality filters.
            var_filters: Optional dict of instance-level equality filters
                (e.g., {"var1": 12477}).
            include_vars: When True, flattens instance variables (var1/var2/...)
                into the result rows. Missing variables are populated with None
                so downstream consumers can rely on consistent keys.

        Returns:
            A list of dictionaries sorted by start_time then instance index.
        """
        if start and end and start >= end:
            raise ValueError("Start time must be before end time")

        template_id_set = set(template_ids) if template_ids else None
        substring = template_substring.lower() if template_substring else None
        var_filters = var_filters or {}

        bucket_rows = self._fetch_bucket_rows(
            columns=["bucket_id", "start_time", "end_time", "templates"],
            bucket_filters=bucket_filters or {},
        )

        results = self._extract_template_instances(
            bucket_rows, template_id_set, substring, var_filters, start, end, include_vars
        )

        if not results:
            return []

        if include_vars:
            var_key_union = self._collect_var_keys(results, var_filters)
            self._flatten_vars_in_results(results, var_key_union)
        else:
            self._remove_vars_from_results(results)

        results.sort(key=lambda r: (r["start_time"], r["instance_index"]))
        return results

    @deal.pre(lambda _, start=None, end=None, **__: start is None or end is None or start < end)
    @deal.post(lambda result: isinstance(result, dict))
    def template_frequency(
        self,
        template_ids: Optional[list[str]] = None,
        template_substring: Optional[str] = None,
        start: Optional[datetime] = None,
        end: Optional[datetime] = None,
        bucket_filters: Optional[dict[str, Any]] = None,
        top_n: Optional[int] = None,
        compare_cohorts: Optional[dict[str, dict[str, Any]]] = None,
    ) -> dict[str, int] | dict[str, dict[str, int]]:
        """Count template instance frequency across buckets.

        Args:
            template_ids: Optional list of template IDs to include.
            template_substring: Optional substring filter for template strings.
            start: Optional inclusive lower bound for bucket overlap window.
            end: Optional exclusive upper bound for bucket overlap window.
            bucket_filters: Optional dict of bucket-level equality filters.
            top_n: Optional limit to top N most frequent templates.
            compare_cohorts: Optional dict mapping cohort names to bucket filters
                for comparison (e.g., {"stuck": {"has_spike": True}}).

        Returns:
            If compare_cohorts provided: dict mapping cohort names to frequency dicts.
            Otherwise: dict mapping template_id to instance count.
        """
        if compare_cohorts:
            return self._frequency_cohort_comparison(
                template_ids, template_substring, start, end, compare_cohorts
            )

        instances = self.template_instances(
            template_ids=template_ids,
            template_substring=template_substring,
            start=start,
            end=end,
            bucket_filters=bucket_filters,
        )

        frequency: dict[str, int] = {}
        for instance in instances:
            template_id = instance["template_id"]
            frequency[template_id] = frequency.get(template_id, 0) + 1

        if top_n:
            sorted_items = sorted(frequency.items(), key=lambda x: x[1], reverse=True)
            frequency = dict(sorted_items[:top_n])

        return frequency

    def _frequency_cohort_comparison(
        self,
        template_ids: Optional[list[str]],
        template_substring: Optional[str],
        start: Optional[datetime],
        end: Optional[datetime],
        compare_cohorts: dict[str, dict[str, Any]],
    ) -> dict[str, dict[str, int]]:
        """Compare template frequencies across cohorts."""
        result: dict[str, dict[str, int]] = {}

        for cohort_name, cohort_filters in compare_cohorts.items():
            frequency = self.template_frequency(
                template_ids=template_ids,
                template_substring=template_substring,
                start=start,
                end=end,
                bucket_filters=cohort_filters,
            )
            result[cohort_name] = frequency

        return result

    @deal.pre(lambda _, start=None, end=None, **__: start is None or end is None or start < end)
    @deal.post(lambda result: isinstance(result, list))
    @deal.post(lambda result: all(isinstance(r, dict) for r in result))
    def timeline_export(
        self,
        template_ids: Optional[list[str]] = None,
        template_substring: Optional[str] = None,
        start: Optional[datetime] = None,
        end: Optional[datetime] = None,
        bucket_filters: Optional[dict[str, Any]] = None,
    ) -> list[dict[str, Any]]:
        """Export timeline-friendly data with bucket metrics and template instances.

        Args:
            template_ids: Optional list of template IDs to include.
            template_substring: Optional substring filter for template strings.
            start: Optional inclusive lower bound for bucket overlap window.
            end: Optional exclusive upper bound for bucket overlap window.
            bucket_filters: Optional dict of bucket-level equality filters.

        Returns:
            List of dicts with bucket metrics and template instance data,
            sorted chronologically.
        """
        instances = self.template_instances(
            template_ids=template_ids,
            template_substring=template_substring,
            start=start,
            end=end,
            bucket_filters=bucket_filters,
            include_vars=True,
        )

        if not instances:
            return []

        bucket_ids = {inst["bucket_id"] for inst in instances}
        bucket_metrics = self._fetch_bucket_metrics(bucket_ids)

        timeline: list[dict[str, Any]] = []
        for instance in instances:
            bucket_id = instance["bucket_id"]
            metrics = bucket_metrics.get(bucket_id, {})
            row = {**metrics, **instance}
            if row.get("start_time") and row["start_time"].tzinfo is None:
                row["start_time"] = row["start_time"].replace(tzinfo=timezone.utc)
            if row.get("end_time") and row["end_time"].tzinfo is None:
                row["end_time"] = row["end_time"].replace(tzinfo=timezone.utc)
            timeline.append(row)

        timeline.sort(key=lambda r: r["start_time"])
        return timeline

    def _fetch_bucket_metrics(self, bucket_ids: set[str]) -> dict[str, dict[str, Any]]:
        """Fetch bucket metrics for given bucket IDs."""
        if not bucket_ids:
            return {}

        placeholders = ", ".join("?" * len(bucket_ids))
        query = f"""
            SELECT bucket_id, start_time, end_time, total_events, error_count,
                   warning_count, info_count, cpu_util, memory_util, disk_util,
                   read_latency_ms, write_latency_ms, has_spike, rebuild_active
            FROM read_parquet('{self._parquet_path.as_posix()}')
            WHERE bucket_id IN ({placeholders})
        """

        with duckdb.connect(":memory:") as conn:
            arrow_table = conn.execute(query, list(bucket_ids)).fetch_arrow_table()

        metrics: dict[str, dict[str, Any]] = {}
        for row in arrow_table.to_pylist():
            bucket_id = row["bucket_id"]
            metrics[bucket_id] = row

        return metrics

    @deal.pre(
        lambda _, _anchor=None, lookback_minutes=None, **__: lookback_minutes is not None
        and lookback_minutes > 0
    )
    @deal.post(lambda result: isinstance(result, list))
    @deal.post(lambda result: all(isinstance(r, dict) for r in result))
    def precursor_sweep(
        self,
        anchor_template_id: str,
        lookback_minutes: int,
        bucket_filters: Optional[dict[str, Any]] = None,
    ) -> list[dict[str, Any]]:
        """Find templates that precede anchor template within lookback window.

        Args:
            anchor_template_id: Template ID to use as anchor (e.g., "970").
            lookback_minutes: Minutes to look back from each anchor instance.
            bucket_filters: Optional dict of bucket-level equality filters.

        Returns:
            List of dicts with template_id and frequency, sorted by frequency descending.
        """
        anchor_instances = self.template_instances(
            template_ids=[anchor_template_id],
            bucket_filters=bucket_filters,
        )

        if not anchor_instances:
            return []

        precursor_counts: dict[str, int] = {}

        for anchor in anchor_instances:
            anchor_time = anchor["start_time"]
            lookback_start = anchor_time - timedelta(minutes=lookback_minutes)

            preceding_instances = self.template_instances(
                start=lookback_start,
                end=anchor_time,
                bucket_filters=bucket_filters,
            )

            seen_templates: set[str] = set()
            for instance in preceding_instances:
                template_id = instance["template_id"]
                if template_id == anchor_template_id:
                    continue
                if template_id not in seen_templates:
                    precursor_counts[template_id] = precursor_counts.get(template_id, 0) + 1
                    seen_templates.add(template_id)

        precursors = [
            {"template_id": tid, "frequency": count}
            for tid, count in precursor_counts.items()
        ]
        precursors.sort(key=lambda x: x["frequency"], reverse=True)

        return precursors

    @deal.post(lambda result: isinstance(result, list))
    def _extract_template_instances(
        self,
        bucket_rows: list[dict[str, Any]],
        template_id_set: Optional[set[str]],
        substring: Optional[str],
        var_filters: dict[str, Any],
        start: Optional[datetime],
        end: Optional[datetime],
        include_vars: bool,
    ) -> list[dict[str, Any]]:
        """Extract template instances from bucket rows matching filters."""
        results: list[dict[str, Any]] = []

        for row in bucket_rows:
            if not self._bucket_overlaps(start, end, row["start_time"], row["end_time"]):
                continue

            templates_data = BucketSelector._parse_templates_payload(row.get("templates"))
            if not templates_data:
                continue

            bucket_results = self._process_templates_in_bucket(
                row, templates_data, template_id_set, substring, var_filters, include_vars
            )
            results.extend(bucket_results)

        return results

    @staticmethod
    def _parse_templates_payload(
        payload: Union[str, dict[str, Any], None]
    ) -> Optional[dict[str, Any]]:
        """Parse templates payload from string JSON or dict."""
        if not payload:
            return None

        if isinstance(payload, str):
            try:
                return json.loads(payload) or {}
            except json.JSONDecodeError:
                return None

        return payload if isinstance(payload, dict) else None

    @deal.post(lambda result: isinstance(result, list))
    def _process_templates_in_bucket(
        self,
        row: dict[str, Any],
        templates_data: dict[str, Any],
        template_id_set: Optional[set[str]],
        substring: Optional[str],
        var_filters: dict[str, Any],
        include_vars: bool,
    ) -> list[dict[str, Any]]:
        """Process all templates in a bucket row."""
        results: list[dict[str, Any]] = []

        for template_id, template_info in templates_data.items():
            if not self._should_include_template(
                template_id, template_info, template_id_set, substring
            ):
                continue

            instances = self._get_template_instances(template_info)
            template_str = self._get_template_str(template_info)

            for index, instance in enumerate(instances):
                instance_dict = self._normalize_instance(instance)
                if not self._should_include_instance(instance_dict, var_filters):
                    continue

                record = self._build_instance_record(
                    row, template_id, template_str, index, instance_dict
                )
                results.append(record)

        return results

    @staticmethod
    def _should_include_template(
        template_id: str,
        template_info: dict[str, Any],
        template_id_set: Optional[set[str]],
        substring: Optional[str],
    ) -> bool:
        """Check if template matches ID and substring filters."""
        if template_id_set and template_id not in template_id_set:
            return False

        if substring:
            template_str = template_info.get("template_str")
            if substring not in (template_str or "").lower():
                return False

        return True

    @staticmethod
    def _get_template_instances(template_info: dict[str, Any]) -> list[dict[str, Any]]:
        """Extract instances list from template info."""
        return template_info.get("instances") or []

    @staticmethod
    def _get_template_str(template_info: dict[str, Any]) -> Optional[str]:
        """Extract template string from template info."""
        return template_info.get("template_str")

    @staticmethod
    def _normalize_instance(instance: Union[dict[str, Any], object]) -> dict[str, Any]:
        """Normalize instance to dict."""
        return instance if isinstance(instance, dict) else {}

    @staticmethod
    def _should_include_instance(
        instance_dict: dict[str, Any], var_filters: dict[str, Any]
    ) -> bool:
        """Check if instance matches variable filters."""
        if not var_filters:
            return True
        return BucketSelector._instance_matches(instance_dict, var_filters)

    @staticmethod
    def _build_instance_record(
        row: dict[str, Any],
        template_id: str,
        template_str: Optional[str],
        index: int,
        instance_dict: dict[str, Any],
    ) -> dict[str, Any]:
        """Build a single instance record."""
        return {
            "bucket_id": row["bucket_id"],
            "start_time": row["start_time"],
            "end_time": row["end_time"],
            "template_id": template_id,
            "template_str": template_str,
            "instance_index": index,
            "_vars": instance_dict,
        }

    @staticmethod
    def _collect_var_keys(results: list[dict[str, Any]], var_filters: dict[str, Any]) -> set[str]:
        """Collect all variable keys from results and filters."""
        var_key_union = set(var_filters.keys())
        for row in results:
            instance_vars = row.get("_vars", {})
            var_key_union.update(instance_vars.keys())
        return var_key_union

    def _flatten_vars_in_results(
        self, results: list[dict[str, Any]], var_key_union: set[str]
    ) -> None:
        """Flatten variable keys into result rows."""
        for row in results:
            instance_vars = row.pop("_vars", {})
            for key in var_key_union:
                row[key] = instance_vars.get(key)

    @staticmethod
    def _remove_vars_from_results(results: list[dict[str, Any]]) -> None:
        """Remove _vars key from result rows."""
        for row in results:
            row.pop("_vars", None)

    def _fetch_bucket_rows(
        self,
        columns: list[str],
        bucket_filters: dict[str, Any],
    ) -> list[dict[str, Any]]:
        selected_columns = ", ".join(columns)
        query = f"SELECT {selected_columns} FROM read_parquet('{self._parquet_path.as_posix()}')"

        conditions: list[str] = []
        params: list[Any] = []

        for field, value in bucket_filters.items():
            if value is None:
                conditions.append(f"{field} IS NULL")
            else:
                conditions.append(f"{field} = ?")
                params.append(value)

        if conditions:
            query += " WHERE " + " AND ".join(conditions)

        with duckdb.connect(":memory:") as conn:
            arrow_table = conn.execute(query, params).fetch_arrow_table()

        return arrow_table.to_pylist()

    @staticmethod
    def _instance_matches(instance: dict[str, Any], filters: dict[str, Any]) -> bool:
        for key, expected in filters.items():
            if key not in instance:
                return False
            if instance.get(key) != expected:
                return False
        return True

    @staticmethod
    def _bucket_overlaps(
        start: Optional[datetime],
        end: Optional[datetime],
        bucket_start: datetime,
        bucket_end: datetime,
    ) -> bool:
        if start is None and end is None:
            return True

        bucket_start_ts = BucketSelector._to_epoch(bucket_start)
        bucket_end_ts = BucketSelector._to_epoch(bucket_end)

        if start is not None and end is not None:
            end_ts = BucketSelector._to_epoch(end)
            start_ts = BucketSelector._to_epoch(start)
            return bucket_start_ts < end_ts and bucket_end_ts > start_ts
        if start is not None:
            return bucket_end_ts > BucketSelector._to_epoch(start)
        if end is not None:
            return bucket_start_ts < BucketSelector._to_epoch(end)
        return True

    @staticmethod
    def _to_epoch(value: datetime) -> float:
        if value.tzinfo is None:
            value = value.replace(tzinfo=timezone.utc)
        return value.timestamp()
