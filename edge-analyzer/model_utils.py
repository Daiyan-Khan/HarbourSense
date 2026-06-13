import os

import joblib
import numpy as np
from sklearn.ensemble import IsolationForest

MODEL_PATH = os.environ.get("ANOMALY_MODEL_PATH", "anomaly_model.pkl")
FEATURE_ORDER = ("motorTemp", "vibration", "energyUse")
MODEL_METADATA = {
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
    np.random.seed(random_state)
    normal_data = np.random.rand(10000, 3)
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
    missing = [name for name in FEATURE_ORDER if name not in payload]
    if missing:
        raise KeyError(f"missing fields: {', '.join(missing)}")

    values = []
    for name in FEATURE_ORDER:
        try:
            values.append(float(payload[name]))
        except (TypeError, ValueError) as error:
            raise ValueError(f"{name} must be numeric") from error

    return np.array([values])
