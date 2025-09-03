from __future__ import annotations

from typing import Any, Dict, List, Iterator

import numpy as np
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName


@model(
    "dar.uss.northwind__customer_segments",
    # Full refresh each run so the whole population is rescored.
    kind=dict(name=ModelKindName.FULL),
    cron="@daily",
    columns={
        "_pit_hook__customer__id": "string",
        "lifetime_value": "double",
        "historical_revenue": "double",
        "active_months": "bigint",
        "value_rate": "double",
        "cluster": "int",
        "segment": "text",
        "silhouette": "double",
        "_record__updated_at": "timestamp",
        "_record__valid_from": "timestamp",
        "_record__valid_to": "timestamp",
        "_record__is_current": "boolean"
    },
)
def execute(context: ExecutionContext, **kwargs: Any) -> Iterator[pd.DataFrame]:
    """
    Segment customers using k-means with k-means++ init and silhouette-based k selection.

    Source model: sushimoderate.customer_lifetime_value
    Features: log1p(lifetime_value), log1p(historical_revenue), log1p(value_rate), active_months
    Weights: lifetime_value emphasized so 'Top' aligns with monetary value.
    """

    # Resolve the upstream table name for the current environment and register dependency.
    clv_table = context.resolve_table("dar.uss.northwind__customer_lifetime_value")

    src_sql = f"""
        SELECT
            _pit_hook__customer__id,
            lifetime_value,
            historical_revenue,
            active_months,
            _record__updated_at,
            _record__valid_from,
            _record__valid_to,
            _record__is_current
        FROM {clv_table}
        WHERE
            _record__is_current = 1
    """
    df = context.fetchdf(src_sql)

    # If upstream has no rows, yield nothing (SQLMesh treats this as an empty result with the declared schema).
    if df is None or df.empty:
        return

    # Basic typing / cleaning
    df["_pit_hook__customer__id"] = df["_pit_hook__customer__id"].astype(str)
    df["lifetime_value"] = df["lifetime_value"].astype(float)
    df["historical_revenue"] = df["historical_revenue"].astype(float)
    df["active_months"] = df["active_months"].fillna(0).astype(int)

    # Feature engineering
    df["value_rate"] = df["historical_revenue"] / np.maximum(df["active_months"], 1)

    # Build feature matrix: log1p-transform skewed monetary features, keep active_months as-is
    feats = np.column_stack(
        [
            np.log1p(df["lifetime_value"].to_numpy()),
            np.log1p(df["historical_revenue"].to_numpy()),
            np.log1p(df["value_rate"].to_numpy()),
            df["active_months"].to_numpy().astype(float),
        ]
    )

    # Standardize
    mu = feats.mean(axis=0)
    sigma = feats.std(axis=0)
    sigma[sigma == 0.0] = 1.0
    X = (feats - mu) / sigma

    # Emphasize monetary value dimensions
    weights = np.array([2.0, 1.0, 1.2, 0.6])  # lifetime_value gets most weight
    Xw = X * weights

    rng = np.random.default_rng(42)

    def kmeans_pp_init(x: np.ndarray, k: int) -> np.ndarray:
        n = x.shape[0]
        centers = np.empty((k, x.shape[1]), dtype=float)
        # First center
        idx = rng.integers(0, n)
        centers[0] = x[idx]
        # Subsequent centers
        closest_sq = ((x - centers[0]) ** 2).sum(axis=1)
        for j in range(1, k):
            probs = closest_sq / closest_sq.sum()
            idx = rng.choice(n, p=probs)
            centers[j] = x[idx]
            d2 = ((x - centers[j]) ** 2).sum(axis=1)
            closest_sq = np.minimum(closest_sq, d2)
        return centers

    def kmeans_fit(x: np.ndarray, k: int, n_init: int = 8, max_iter: int = 200, tol: float = 1e-6):
        best_inertia = np.inf
        best_labels = None
        best_centers = None
        for _ in range(n_init):
            centers = kmeans_pp_init(x, k)
            labels = np.zeros(x.shape[0], dtype=int)
            for _ in range(max_iter):
                # Assign
                d2 = ((x[:, None, :] - centers[None, :, :]) ** 2).sum(axis=2)
                new_labels = d2.argmin(axis=1)
                # Update
                new_centers = np.stack(
                    [x[new_labels == j].mean(axis=0) if np.any(new_labels == j) else centers[j] for j in range(k)]
                )
                if np.linalg.norm(new_centers - centers) < tol:
                    labels = new_labels
                    centers = new_centers
                    break
                labels = new_labels
                centers = new_centers
            inertia = ((x - centers[labels]) ** 2).sum()
            if inertia < best_inertia:
                best_inertia = inertia
                best_labels = labels
                best_centers = centers
        return best_labels, best_centers, float(best_inertia)

    def silhouette_scores(x: np.ndarray, labels: np.ndarray) -> np.ndarray:
        # Pairwise Euclidean distances (OK for small N like this example)
        n = x.shape[0]
        sum_sq = (x ** 2).sum(axis=1, keepdims=True)
        d2 = sum_sq + sum_sq.T - 2 * (x @ x.T)
        d2 = np.maximum(d2, 0.0)
        d = np.sqrt(d2)

        s = np.zeros(n, dtype=float)
        for i in range(n):
            same = labels == labels[i]
            other = ~same
            # a: mean distance to same-cluster points (exclude self)
            if same.sum() > 1:
                a = d[i, same].sum() / (same.sum() - 1)
            else:
                a = 0.0
            # b: minimal mean distance to points in other clusters
            b = np.inf
            for cl in np.unique(labels[other]):
                mask = labels == cl
                b = min(b, d[i, mask].mean())
            s[i] = 0.0 if max(a, b) == 0 else (b - a) / max(a, b)
        return s

    # Choose k by silhouette in [3..6], bounded by n
    n = Xw.shape[0]
    k_candidates = [k for k in range(3, 7) if k <= n]
    if not k_candidates:
        # Fallback: single cluster
        df["cluster"] = 0
        df["segment"] = "Top"
        df["silhouette"] = 0.0
    else:
        best = None
        best_score = -np.inf
        for k in k_candidates:
            labels, centers, _ = kmeans_fit(Xw, k=k)
            s = silhouette_scores(Xw, labels)
            score = float(np.nan_to_num(s).mean())
            if score > best_score:
                best = (k, labels, s)
                best_score = score

        k, labels, s = best
        df["cluster"] = labels.astype(int)
        df["silhouette"] = s.astype(float)

        # Label clusters by ascending mean lifetime_value
        means = df.groupby("cluster")["lifetime_value"].mean().sort_values()
        order = list(means.index)  # ascending by LTV
        names_by_k: Dict[int, List[str]] = {
            3: ["Low", "Mid", "Top"],
            4: ["Low", "Mid", "High", "Top"],
            5: ["Very Low", "Low", "Mid", "High", "Top"],
            6: ["Very Low", "Low", "Mid", "High", "Very High", "Top"],
        }
        labels_for_k = names_by_k.get(k, ["Low", "Mid", "High", "Top"][:k])
        name_map = {cl: labels_for_k[i] for i, cl in enumerate(order)}
        df["segment"] = df["cluster"].map(name_map).astype(str)

    out = df[
        [
            "_pit_hook__customer__id",
            "lifetime_value",
            "historical_revenue",
            "active_months",
            "value_rate",
            "cluster",
            "segment",
            "silhouette",
            "_record__updated_at",
            "_record__valid_from",
            "_record__valid_to",
            "_record__is_current"
        ]
    ].copy()

    # Ensure declared types
    out["_pit_hook__customer__id"] = out["_pit_hook__customer__id"].astype(str)
    out["active_months"] = out["active_months"].astype(int)
    out["cluster"] = out["cluster"].astype(int)
    out["lifetime_value"] = out["lifetime_value"].astype(float)
    out["historical_revenue"] = out["historical_revenue"].astype(float)
    out["value_rate"] = out["value_rate"].astype(float)
    out["silhouette"] = out["silhouette"].astype(float)
    out["segment"] = out["segment"].astype(str)

    # Yield the final frame (chunked-output friendly). If no rows upstream, we yielded nothing above.
    yield out