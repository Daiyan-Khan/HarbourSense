# edge-analyzer/create_model.py

import numpy as np
from sklearn.ensemble import IsolationForest
import joblib

print("Starting model training...")

# Step 1: Generate sample training data.
# In a real-world scenario, you would use a large dataset of *normal*
# operational telemetry from your cranes. For now, we'll simulate this.
# Let's assume our features are [motorTemp, vibration, energyUse].
np.random.seed(42)
# 10,000 samples of normal data
normal_data = np.random.rand(10000, 3)
normal_data[:, 0] = normal_data[:, 0] * 10 + 80  # Normal temp: 80-90°C
normal_data[:, 1] = normal_data[:, 1] * 0.5 + 0.1 # Normal vibration: 0.1-0.6
normal_data[:, 2] = normal_data[:, 2] * 20 + 100 # Normal energy: 100-120W

# Step 2: Initialize and train the Isolation Forest model.
# The 'contamination' parameter tells the model what proportion of the data
# is expected to be anomalous. We set it to a low value.
model = IsolationForest(n_estimators=100, contamination=0.01, random_state=42)

print("Fitting model to the training data...")
model.fit(normal_data)
print("Model training complete.")

# Step 3: Save the trained model to a file.
# This creates the anomaly_model.pkl file that your analyzer needs.
joblib.dump(model, 'anomaly_model.pkl')

print("Successfully trained and saved model to anomaly_model.pkl")

