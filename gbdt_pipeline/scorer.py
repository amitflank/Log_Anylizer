"""Scoring and SHAP helpers for the lightweight GBDT pipeline."""

from __future__ import annotations

import json
from collections.abc import Iterator
from dataclasses import dataclass
from pathlib import Path

import lightgbm as lgb
import numpy as np
import pandas as pd

try:  # pragma: no cover - optional dependency validated in CI
    import shap
except ImportError:  # pragma: no cover
    shap = None


@dataclass(frozen=True)
class _ModelMetadata:
    feature_columns: list[str]
    booster_path: Path
    feature_name_map: dict[str, str]


class GBDTScorer:
    """Apply a trained LightGBM model and expose SHAP explanations."""

    def __init__(self, model_path: Path, feature_columns: list[str] | None = None) -> None:
        """Load the model, metadata, and remember feature ordering."""
        metadata = _load_model_metadata(Path(model_path))
        self._booster = lgb.Booster(model_file=str(metadata.booster_path))
        self._features = list(feature_columns or metadata.feature_columns)
        if not self._features:
            raise ValueError("Feature columns must be provided")
        self._feature_name_map = dict(metadata.feature_name_map)
        self._explainer: shap.TreeExplainer | None = None

    def score(self, dataframe: pd.DataFrame) -> pd.DataFrame:
        """Return probabilities for each row in the dataframe."""
        probabilities = self._predict_probabilities(dataframe)
        bucket_ids = (
            dataframe["bucket_id"].astype(str).tolist()
            if "bucket_id" in dataframe.columns
            else [str(idx) for idx in range(len(probabilities))]
        )
        return pd.DataFrame({"bucket_id": bucket_ids, "probability": probabilities})

    def _predict_probabilities(self, dataframe: pd.DataFrame) -> np.ndarray:
        features = _coerce_numeric_frame(dataframe[self._features])
        sanitized = features.rename(columns=self._feature_name_map)
        return self._booster.predict(sanitized)

    def shap_values(self, dataframe: pd.DataFrame) -> tuple[float, pd.DataFrame]:
        """Return expected value and SHAP contributions for LightGBM models."""
        if shap is None:  # pragma: no cover - enforced via packaging
            raise RuntimeError(
                "The `shap` extra is required for explanations; install with `pip install shap`."
            )
        features = _coerce_numeric_frame(dataframe[self._features])
        sanitized = features.rename(columns=self._feature_name_map)
        if self._explainer is None:
            self._explainer = shap.TreeExplainer(self._booster)
        shap_values = self._explainer.shap_values(sanitized)
        expected_value = self._explainer.expected_value
        shap_values, expected_value = _normalise_shap_output(shap_values, expected_value)
        contributions = pd.DataFrame(shap_values, columns=sanitized.columns, index=sanitized.index)
        inverse_map = {v: k for k, v in self._feature_name_map.items()}
        contributions = contributions.rename(columns=inverse_map)
        contributions = contributions[self._features]
        return expected_value, contributions

    def stream_shap_values(
        self, dataframe: pd.DataFrame, batch_size: int = 2000
    ) -> Iterator[tuple[float, pd.DataFrame]]:
        """Yield SHAP contributions in bounded batches to control memory usage."""
        for chunk in _chunk_dataframe(dataframe, batch_size):
            yield self._explain_chunk(chunk)

    def shap_interaction_values(
        self, dataframe: pd.DataFrame, top_k_features: int = 30
    ) -> tuple[float, np.ndarray, list[str]]:
        """Return expected value and interaction matrix for the top-k variance features."""
        if shap is None:  # pragma: no cover - enforced via packaging
            raise RuntimeError(
                "The `shap` extra is required for interactions; install with `pip install shap`."
            )
        top_features = self._top_k_features_by_shap_variance(dataframe, top_k_features)
        if not top_features:
            return 0.0, np.zeros((0, 0, 0)), []

        sanitized = _coerce_numeric_frame(dataframe[top_features])
        sanitized = sanitized.rename(columns=self._feature_name_map)
        if self._explainer is None:
            self._explainer = shap.TreeExplainer(self._booster)

        interactions = self._explainer.shap_interaction_values(sanitized)
        expected_value = self._explainer.expected_value
        interactions = _normalise_shap_interaction_output(interactions)
        expected_value = _normalise_expected_value(expected_value)
        return expected_value, interactions, top_features

    def stream_interaction_values(
        self,
        dataframe: pd.DataFrame,
        batch_size: int = 500,
        top_k_features: int = 30,
    ) -> Iterator[tuple[float, np.ndarray, list[str]]]:
        """Yield interaction matrices in bounded batches using a fixed top-k feature set."""
        if shap is None:  # pragma: no cover - enforced via packaging
            raise RuntimeError(
                "The `shap` extra is required for interactions; install with `pip install shap`."
            )
        top_features = self._top_k_features_by_shap_variance(dataframe, top_k_features)
        if not top_features:
            return

        if self._explainer is None:
            self._explainer = shap.TreeExplainer(self._booster)

        for chunk in _chunk_dataframe(dataframe, batch_size):
            sanitized = _coerce_numeric_frame(chunk[top_features])
            sanitized = sanitized.rename(columns=self._feature_name_map)
            interactions = self._explainer.shap_interaction_values(sanitized)
            expected_value = self._explainer.expected_value
            interactions = _normalise_shap_interaction_output(interactions)
            expected_value = _normalise_expected_value(expected_value)
            yield expected_value, interactions, top_features

    def _explain_chunk(self, chunk: pd.DataFrame) -> tuple[float, pd.DataFrame]:
        expected, contrib_df = self.shap_values(chunk)
        return expected, contrib_df.reset_index(drop=True)

    def _top_k_features_by_shap_variance(
        self, dataframe: pd.DataFrame, top_k_features: int
    ) -> list[str]:
        """Select top-k features by SHAP variance using streamed SHAP main effects."""
        if top_k_features <= 0:
            return []

        sums = np.zeros(len(self._features), dtype=float)
        sums_sq = np.zeros(len(self._features), dtype=float)
        count = 0

        for _, contrib_df in self.stream_shap_values(dataframe, batch_size=2000):
            values = contrib_df[self._features].to_numpy(dtype=float)
            sums += values.sum(axis=0)
            sums_sq += np.square(values).sum(axis=0)
            count += len(values)

        if count == 0:
            return []

        mean = sums / count
        variance = sums_sq / count - np.square(mean)
        top_indices = np.argsort(variance)[::-1][: min(top_k_features, len(variance))]
        return [self._features[idx] for idx in top_indices]


def _load_model_metadata(path: Path) -> _ModelMetadata:
    payload = json.loads(path.read_text())
    booster_name = payload.get("booster_path")
    booster_path = path.with_name(booster_name) if booster_name else path.with_suffix(".txt")
    feature_columns = payload.get("feature_columns") or []
    feature_name_map = {str(k): str(v) for k, v in (payload.get("feature_name_map") or {}).items()}
    return _ModelMetadata(
        feature_columns=feature_columns,
        booster_path=booster_path,
        feature_name_map=feature_name_map,
    )


def _chunk_dataframe(dataframe: pd.DataFrame, batch_size: int) -> Iterator[pd.DataFrame]:
    for start in range(0, len(dataframe), batch_size):
        yield dataframe.iloc[start : start + batch_size]


def _normalise_shap_output(
    shap_values: np.ndarray | list[np.ndarray], expected_value: float | list[float] | np.ndarray
) -> tuple[np.ndarray, float]:
    if isinstance(shap_values, list):
        shap_values = shap_values[-1]
    expected_value = _normalise_expected_value(expected_value)
    return np.asarray(shap_values), expected_value


def _normalise_shap_interaction_output(interactions: np.ndarray | list[np.ndarray]) -> np.ndarray:
    if isinstance(interactions, list):
        interactions = interactions[-1]
    return np.asarray(interactions)


def _normalise_expected_value(expected_value: float | list[float] | np.ndarray) -> float:
    if isinstance(expected_value, (list, np.ndarray)):
        expected_value = float(np.asarray(expected_value).flatten()[-1])
    return float(expected_value)


def _coerce_numeric_frame(frame: pd.DataFrame) -> pd.DataFrame:
    numeric = frame.apply(pd.to_numeric, errors="coerce")
    return numeric.fillna(0.0)
