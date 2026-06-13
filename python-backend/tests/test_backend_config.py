import unittest

from pathlib import Path
import sys


BACKEND_DIR = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(BACKEND_DIR))


import backend_config


class MongoConfigTests(unittest.TestCase):
    def test_defaults_match_compose_contract(self):
        settings = backend_config.get_mongo_settings({})

        self.assertEqual(settings.uri, "mongodb://host.docker.internal:27017/port")
        self.assertEqual(settings.database_name, "port")
        self.assertEqual(settings.server_selection_timeout_ms, 5000)

    def test_default_uri_can_be_built_from_host_port_and_database(self):
        settings = backend_config.get_mongo_settings(
            {
                "MONGO_HOST": "mongo",
                "MONGO_PORT": "27017",
                "MONGO_DB_NAME": "port",
            }
        )

        self.assertEqual(settings.uri, "mongodb://mongo:27017/port")

    def test_accepts_explicit_uri_database_and_timeout(self):
        settings = backend_config.get_mongo_settings(
            {
                "MONGO_URI": "mongodb://mongo.example.test:27017/port",
                "MONGO_DB_NAME": "harbour",
                "MONGO_SERVER_SELECTION_TIMEOUT_MS": "1500",
            }
        )

        self.assertEqual(settings.uri, "mongodb://mongo.example.test:27017/port")
        self.assertEqual(settings.database_name, "harbour")
        self.assertEqual(settings.server_selection_timeout_ms, 1500)

    def test_rejects_missing_uri_host(self):
        with self.assertRaisesRegex(backend_config.MongoConfigError, "missing a MongoDB host"):
            backend_config.get_mongo_settings({"MONGO_URI": "mongodb://", "MONGO_DB_NAME": "port"})

    def test_rejects_placeholder_uri(self):
        with self.assertRaisesRegex(backend_config.MongoConfigError, "placeholder"):
            backend_config.get_mongo_settings(
                {
                    "MONGO_URI": "mongodb+srv://username:password@cluster.example.com/port",
                    "MONGO_DB_NAME": "port",
                }
            )

    def test_client_factory_errors_are_sanitized(self):
        settings = backend_config.get_mongo_settings(
            {
                "MONGO_URI": "mongodb+srv://user:fakepass@missing.mongodb.net/port",
                "MONGO_DB_NAME": "port",
            }
        )

        def failing_factory(*args, **kwargs):
            raise RuntimeError("The DNS query name does not exist")

        with self.assertRaises(backend_config.MongoConfigError) as context:
            backend_config.create_mongo_client(settings, client_factory=failing_factory)

        message = str(context.exception)
        self.assertIn("MongoDB SRV lookup failed", message)
        self.assertIn("RuntimeError", message)
        self.assertNotIn("fakepass", message)
        self.assertNotIn("missing.mongodb.net", message)


if __name__ == "__main__":
    unittest.main()
