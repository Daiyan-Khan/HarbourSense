import json
import os
import tempfile
import unittest

from model_utils import FEATURE_ORDER, analyze_telemetry, ensure_model, extract_features, train_model


class ModelUtilsTests(unittest.TestCase):
    def test_train_model_predicts_inlier_for_normal_sample(self):
        model = train_model(random_state=7)
        sample = [[85.0, 0.3, 110.0]]
        self.assertEqual(model.predict(sample)[0], 1)

    def test_ensure_model_writes_missing_artifact(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            model_path = os.path.join(temp_dir, "anomaly_model.pkl")
            model = ensure_model(model_path)
            self.assertTrue(os.path.exists(model_path))
            self.assertEqual(model.predict([[85.0, 0.3, 110.0]])[0], 1)

    def test_extract_features_rejects_missing_and_non_numeric_fields(self):
        with self.assertRaises(KeyError):
            extract_features({"motorTemp": 85, "vibration": 0.2})
        with self.assertRaises(ValueError):
            extract_features(
                {"motorTemp": "hot", "vibration": 0.2, "energyUse": 110}
            )

    def test_feature_order_matches_contract(self):
        payload = {"motorTemp": 1, "vibration": 2, "energyUse": 3, "craneId": "c1"}
        values = extract_features(payload).tolist()[0]
        self.assertEqual(values, [1.0, 2.0, 3.0])
        self.assertEqual(FEATURE_ORDER, ("motorTemp", "vibration", "energyUse"))

    def test_nonfinite_boolean_and_nonobject_inputs_are_rejected(self):
        for value in (float("nan"), float("inf"), -float("inf"), True):
            with self.subTest(value=value), self.assertRaises(ValueError):
                extract_features({"motorTemp": value, "vibration": 0.3, "energyUse": 110})
        for payload in ([], None, "motorTemp"):
            with self.subTest(payload=payload), self.assertRaises(TypeError):
                extract_features(payload)

    def test_analysis_sign_matches_model_and_explanation_describes_only_observations(self):
        model = train_model()
        for temperature in (85, 200):
            payload = {"motorTemp": temperature, "vibration": 0.3, "energyUse": 110}
            result = analyze_telemetry(payload, model)
            self.assertEqual(result["anomalous"], model.predict(extract_features(payload))[0] == -1)
            self.assertEqual(result["threshold"], 0)
            self.assertTrue(result["syntheticTraining"])
            self.assertIn("not a causal explanation", result["explanation"])
        self.assertEqual(result["deviations"], [{"feature": "motorTemp", "value": 200, "expectedRange": [80, 90], "direction": "above"}])

    def test_model_training_does_not_reseed_callers_rng(self):
        import numpy as np
        np.random.seed(73)
        expected = np.random.rand(3)
        np.random.seed(73)
        train_model(7)
        np.testing.assert_array_equal(np.random.rand(3), expected)


if __name__ == "__main__":
    unittest.main()
