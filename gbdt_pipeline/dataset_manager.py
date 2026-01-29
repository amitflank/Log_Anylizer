"""Utilities for assembling model-ready datasets."""

from __future__ import annotations

import shutil
import tempfile
import uuid
from contextlib import suppress
from dataclasses import dataclass
from pathlib import Path

import pandas as pd

from gbdt_pipeline.feature_builder import FeatureRow
from gbdt_pipeline.label_builder import BucketLabel


@dataclass
class DatasetSplits:
    """Train, validation, and test partitions."""

    train_df: pd.DataFrame
    val_df: pd.DataFrame
    test_df: pd.DataFrame


class DatasetManager:
    """Accumulate labelled examples and produce data splits."""

    def __init__(self, output_dir: Path | None = None, buffer_size: int = 2000) -> None:
        """Initialise the manager and configure disk spilling for large datasets."""
        self._output_dir = Path(output_dir) if output_dir else None
        self._buffer_limit = max(1, buffer_size)
        self._buffer: list[dict[str, object]] = []
        self._chunk_paths: list[Path] = []
        self._spill_dir = self._initialise_spill_dir()

    def add_example(self, feature_row: FeatureRow, bucket_label: BucketLabel) -> None:
        """Append a single feature/label pair."""
        record = {
            "bucket_id": feature_row.bucket_id,
            "timestamp": feature_row.timestamp,
            "label": bucket_label.label,
        }
        record.update(feature_row.values)
        self._buffer.append(record)
        self._flush_buffer()

    def finalise(
        self,
        train_ratio: float = 0.8,
        val_ratio: float = 0.0,
        enforce_label_balance: bool = True,
    ) -> DatasetSplits:
        """Create sorted train/validation/test splits."""
        self._flush_buffer(force=True)
        dataframe = self._load_concatenated_frame()
        if dataframe.empty:
            self._cleanup_spill_dir()
            raise ValueError("No records to finalise")
        if val_ratio < 0 or train_ratio <= 0:
            self._cleanup_spill_dir()
            raise ValueError("train_ratio must be > 0 and val_ratio must be >= 0.")
        if enforce_label_balance and val_ratio > 0:
            self._cleanup_spill_dir()
            raise ValueError("Label-balanced splitting currently supports only train/test splits.")

        dataframe = dataframe.sort_values("timestamp").reset_index(drop=True)
        total = len(dataframe)

        if enforce_label_balance:
            self._cleanup_spill_dir()
            return self._label_balanced_splits(dataframe, train_ratio=train_ratio)

        train_size = max(1, int(total * train_ratio))
        val_size = max(0, int(total * val_ratio))
        remaining = total - train_size - val_size
        test_size = max(1, remaining)

        if val_size > 0 and train_size + val_size + test_size > total:
            val_size = max(0, total - train_size - test_size)

        train_end = train_size
        val_end = train_end + val_size

        train_df = dataframe.iloc[:train_end].copy()
        val_df = dataframe.iloc[train_end:val_end].copy()
        test_df = dataframe.iloc[val_end:].copy()

        splits = DatasetSplits(train_df=train_df, val_df=val_df, test_df=test_df)
        self._cleanup_spill_dir()
        return splits

    def time_series_folds(
        self,
        n_splits: int,
        *,
        test_size: int | None = None,
        min_train_size: int | None = None,
    ) -> list[DatasetSplits]:
        """Create expanding-window time-series folds (train/test only)."""
        self._flush_buffer(force=True)
        dataframe = self._load_concatenated_frame()
        if dataframe.empty:
            self._cleanup_spill_dir()
            raise ValueError("No records to finalise")
        if n_splits < 1:
            self._cleanup_spill_dir()
            raise ValueError("n_splits must be >= 1.")

        dataframe = dataframe.sort_values("timestamp").reset_index(drop=True)
        total = len(dataframe)
        if test_size is None:
            test_size = max(1, total // (n_splits + 1))
        if test_size <= 0:
            self._cleanup_spill_dir()
            raise ValueError("test_size must be >= 1.")

        first_test_start = total - n_splits * test_size
        if first_test_start < 1:
            self._cleanup_spill_dir()
            raise ValueError("Not enough records to create the requested time-series folds.")

        if min_train_size is None:
            min_train_size = max(1, first_test_start)
        if min_train_size < 1:
            self._cleanup_spill_dir()
            raise ValueError("min_train_size must be >= 1.")
        if first_test_start < min_train_size:
            self._cleanup_spill_dir()
            raise ValueError("Not enough records to satisfy min_train_size.")

        folds: list[DatasetSplits] = []
        for idx in range(n_splits):
            test_start = first_test_start + idx * test_size
            test_end = test_start + test_size
            train_df = dataframe.iloc[:test_start].copy()
            test_df = dataframe.iloc[test_start:test_end].copy()
            folds.append(
                DatasetSplits(
                    train_df=train_df.reset_index(drop=True),
                    val_df=pd.DataFrame(),
                    test_df=test_df.reset_index(drop=True),
                )
            )

        self._cleanup_spill_dir()
        return folds

    def _label_balanced_splits(
        self,
        dataframe: pd.DataFrame,
        train_ratio: float,
    ) -> DatasetSplits:
        """Create label-aware train/test splits (no validation)."""
        labels = sorted(dataframe["label"].unique())
        train_parts: list[pd.DataFrame] = []
        test_parts: list[pd.DataFrame] = []

        for label in labels:
            subset = dataframe[dataframe["label"] == label]
            count = len(subset)
            if count == 0:
                continue

            train_count = max(1, int(count * train_ratio))
            test_count = count - train_count
            if test_count == 0 and count > 1:
                train_count = count - 1
                test_count = 1

            train_parts.append(subset.iloc[:train_count])
            test_parts.append(subset.iloc[train_count:train_count + test_count])

        train_df = pd.concat(train_parts).sort_values("timestamp")
        test_df = pd.concat(test_parts).sort_values("timestamp")

        return DatasetSplits(
            train_df=train_df.reset_index(drop=True),
            val_df=pd.DataFrame(),
            test_df=test_df.reset_index(drop=True),
        )

    def _initialise_spill_dir(self) -> Path:
        base_dir = self._output_dir or Path(tempfile.gettempdir())
        spill_dir = base_dir / f"dataset_spill_{uuid.uuid4().hex}"
        spill_dir.mkdir(parents=True, exist_ok=True)
        return spill_dir

    def _flush_buffer(self, force: bool = False) -> None:
        if not self._buffer:
            return
        if len(self._buffer) < self._buffer_limit and not force:
            return
        frame = pd.DataFrame(self._buffer)
        chunk_path = self._spill_dir / f"chunk_{len(self._chunk_paths):04d}.parquet"
        frame.to_parquet(chunk_path, index=False)
        self._chunk_paths.append(chunk_path)
        self._buffer.clear()

    def _load_concatenated_frame(self) -> pd.DataFrame:
        if not self._chunk_paths:
            return pd.DataFrame()
        frames = [pd.read_parquet(path) for path in self._chunk_paths]
        return pd.concat(frames, ignore_index=True)

    def _cleanup_spill_dir(self) -> None:
        self._buffer.clear()
        if self._spill_dir.exists():
            shutil.rmtree(self._spill_dir, ignore_errors=True)
        self._chunk_paths.clear()

    def __del__(self) -> None:
        with suppress(Exception):
            self._cleanup_spill_dir()
