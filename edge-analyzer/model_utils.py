import os

import joblib
import numpy as np
from sklearn.ensemble import IsolationForest

MODEL_PATH = os.environ.get("ANOMALY_MODEL_PATH", "anomaly_model.pkl")
FEATURE_ORDER = ("motorTemp", "vibration", "energyUse")
MODEL_METADATA = {
    "model": "IsolationForest",
    "syntheticTraining": True,
    "trainingSeed": 42,
    "trainingSamples": 10000,
    "featureOrder": list(FEATURE_ORDER),
    "trainingScript": "create_model.py",
    "contamination": 0.01,
    "normalRanges": {
        "motorTemp": [80, 90],
        "vibration": [0.1, 0.6],
        "energyUse": [100, 120],
    },
}


def train_model(random_state=42):
    # Keep the original training distribution without changing callers' random state.
    normal_data = np.random.RandomState(random_state).rand(10000, 3)
    normal_data[:, 0] = normal_data[:, 0] * 10 + 80
    normal_data[:, 1] = normal_data[:, 1] * 0.5 + 0.1
    normal_data[:, 2] = normal_data[:, 2] * 20 + 100

    model = IsolationForest(
        n_estimators=100,
        contamination=MODEL_METADATA["contamination"],
        random_state=random_state,
    )
    model.fit(normal_data)
    return model


def ensure_model(path=MODEL_PATH):
    if os.path.exists(path):
        return joblib.load(path)
    model = train_model()
    joblib.dump(model, path)
    return model


def extract_features(payload):
    if not isinstance(payload, dict):
        raise TypeError("telemetry must be an object")
    missing = [name for name in FEATURE_ORDER if name not in payload]
    if missing:
        raise KeyError(f"missing fields: {', '.join(missing)}")

    values = []
    for name in FEATURE_ORDER:
        try:
            value = float(payload[name])
        except (TypeError, ValueError) as error:
            raise ValueError(f"{name} must be numeric") from error
        if isinstance(payload[name], bool) or not np.isfinite(value):
            raise ValueError(f"{name} must be a finite number")
        values.append(value)

    return np.array([values])


def analyze_telemetry(payload, model):
    """Score telemetry and describe range deviations, without claiming causality.

    The score is negative sklearn decision_function: positive means anomalous.
    Range comparisons describe inputs; they are not model feature attribution.
    """
    features = extract_features(payload)
    values = dict(zip(FEATURE_ORDER, features[0].tolist()))
    score = -float(model.decision_function(features)[0])
    deviations = []
    for name, value in values.items():
        lower, upper = MODEL_METADATA["normalRanges"][name]
        if value < lower or value > upper:
            deviations.append({
                "feature": name,
                "value": value,
                "expectedRange": [lower, upper],
                "direction": "below" if value < lower else "above",
            })
    description = "; ".join(
        f"{item['feature']} {item['value']:g} is {item['direction']} the synthetic "
        f"training range {item['expectedRange'][0]:g}–{item['expectedRange'][1]:g}"
        for item in deviations
    ) or "All values are inside the marginal synthetic training ranges; their joint pattern may still be unusual."
    return {
        "model": "IsolationForest",
        "syntheticTraining": True,
        "anomalyScore": score,
        "threshold": 0,
        "anomalous": score > 0,
        "featureOrder": list(FEATURE_ORDER),
        "observedFeatures": values,
        "deviations": deviations,
        "explanation": description + " This is an observed range comparison, not a causal explanation or advance failure prediction.",
    }
