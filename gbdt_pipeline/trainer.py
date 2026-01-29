"""Training helpers for the lightweight GBDT pipeline."""

from __future__ import annotations

import json
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import lightgbm as lgb
import numpy as np
import pandas as pd


@dataclass
class TrainingConfig:
    """Configuration parameters for model training."""

    target_name: str
    feature_columns: list[str]
    label_column: str
    model_output_path: Path
    lgbm_params: dict[str, Any] | None = None
    lgbm_num_boost_round: int = 100
    balance_classes: bool = False


@dataclass
class TrainingReport:
    """Summary of a training run."""

    model_path: Path
    metrics: dict[str, float]


class GBDTTrainer:
    """Train the lightweight surrogate model."""

    def __init__(self, config: TrainingConfig) -> None:
        """Store configuration."""
        self._config = config

    def train(
        self,
        train_df: pd.DataFrame,
        val_df: pd.DataFrame,
        test_df: pd.DataFrame,
    ) -> TrainingReport:
        """Fit the LightGBM model and compute evaluation metrics."""
        return self._train_lightgbm(train_df, val_df, test_df)

    def _train_lightgbm(
        self,
        train_df: pd.DataFrame,
        val_df: pd.DataFrame,
        test_df: pd.DataFrame,
    ) -> TrainingReport:
        features = self._config.feature_columns
        label_col = self._config.label_column

        x_train = train_df[features].apply(pd.to_numeric, errors="coerce").fillna(0.0)
        y_train = train_df[label_col].astype(float)
        params = {
            "objective": "binary",
            "metric": "auc",
            "boosting": "gbdt",
            "learning_rate": 0.1,
            "num_leaves": 31,
            "min_data_in_leaf": 1,
            "feature_fraction": 0.9,
            "bagging_fraction": 0.8,
            "bagging_freq": 1,
            "lambda_l1": 0.0,
            "lambda_l2": 0.0,
            "max_depth": -1,
            "seed": 2025,
            "verbosity": -1,
        }
        if self._config.lgbm_params:
            params.update(self._config.lgbm_params)

        if self._config.balance_classes and "scale_pos_weight" not in params:
            positives = float(y_train.sum())
            negatives = float(len(y_train) - positives)
            if positives > 0 and negatives > 0:
                params["scale_pos_weight"] = negatives / positives

        name_map = self._sanitize_feature_names(features)
        sanitized_train = x_train.rename(columns=name_map)

        train_set = lgb.Dataset(sanitized_train, label=y_train)

        valid_sets = []
        if not val_df.empty:
            x_val = val_df[features].apply(pd.to_numeric, errors="coerce").fillna(0.0)
            y_val = val_df[label_col].astype(float)
            sanitized_val = x_val.rename(columns=name_map)
            valid_sets.append(lgb.Dataset(sanitized_val, label=y_val, reference=train_set))

        booster = lgb.train(
            params,
            train_set,
            num_boost_round=self._config.lgbm_num_boost_round,
            valid_sets=valid_sets or None,
        )

        booster_path = self._config.model_output_path.with_suffix(".txt")
        booster.save_model(str(booster_path))
        metadata = {
            "model_type": "lightgbm",
            "feature_columns": features,
            "booster_path": booster_path.name,
            "feature_name_map": name_map,
        }
        self._config.model_output_path.write_text(json.dumps(metadata))

        x_test = test_df[features].apply(pd.to_numeric, errors="coerce").fillna(0.0)
        y_test = test_df[label_col].astype(float).to_numpy()
        sanitized_test = x_test.rename(columns=name_map)
        probabilities = booster.predict(sanitized_test)
        auc = self._roc_auc(y_test, probabilities)

        return TrainingReport(
            model_path=self._config.model_output_path,
            metrics={"auc": auc},
        )

    @staticmethod
    def _roc_auc(labels: np.ndarray, scores: np.ndarray) -> float:
        """Compute a simple ROC-AUC score."""
        if len(labels) == 0:
            return 0.5
        order = np.argsort(scores)
        ranks = np.arange(1, len(scores) + 1)
        sorted_labels = labels[order]
        positives = sorted_labels.sum()
        negatives = len(sorted_labels) - positives
        if positives == 0 or negatives == 0:
            return 0.5
        sum_ranks = float(np.sum(ranks * sorted_labels))
        auc = (sum_ranks - positives * (positives + 1) / 2) / (positives * negatives)
        return float(max(0.0, min(1.0, auc)))

    @staticmethod
    def _sanitize_feature_names(columns: list[str]) -> dict[str, str]:
        """Create a LightGBM-friendly feature name mapping."""
        mapping: dict[str, str] = {}
        used: set[str] = set()
        for column in columns:
            sanitized = re.sub(r"[^A-Za-z0-9_]", "_", column)
            if not sanitized:
                sanitized = "f"
            candidate = sanitized
            idx = 1
            while candidate in used:
                candidate = f"{sanitized}_{idx}"
                idx += 1
            mapping[column] = candidate
            used.add(candidate)
        return mapping
