import os
from dataclasses import dataclass
from urllib.parse import urlparse


DEFAULT_MONGO_HOST = "host.docker.internal"
DEFAULT_MONGO_PORT = "27017"
DEFAULT_MONGO_DB_NAME = "port"
DEFAULT_SERVER_SELECTION_TIMEOUT_MS = 5000


class MongoConfigError(RuntimeError):
    """Raised when MongoDB runtime configuration is missing or invalid."""


@dataclass(frozen=True)
class MongoSettings:
    uri: str
    database_name: str
    server_selection_timeout_ms: int = DEFAULT_SERVER_SELECTION_TIMEOUT_MS


def _looks_like_placeholder(value: str) -> bool:
    lowered = value.lower()
    placeholder_markers = (
        "<",
        ">",
        "your-",
        "username:password",
        "user:password",
        "example.com",
    )
    return any(marker in lowered for marker in placeholder_markers)


def _parse_timeout(value: str | None) -> int:
    if not value:
        return DEFAULT_SERVER_SELECTION_TIMEOUT_MS
    try:
        timeout_ms = int(value)
    except ValueError as exc:
        raise MongoConfigError("MONGO_SERVER_SELECTION_TIMEOUT_MS must be an integer number of milliseconds.") from exc
    if timeout_ms <= 0:
        raise MongoConfigError("MONGO_SERVER_SELECTION_TIMEOUT_MS must be greater than zero.")
    return timeout_ms


def build_default_mongo_uri(env: dict[str, str] | None = None) -> str:
    env = os.environ if env is None else env
    host = env.get("MONGO_HOST", DEFAULT_MONGO_HOST).strip()
    port = env.get("MONGO_PORT", DEFAULT_MONGO_PORT).strip()
    database_name = env.get("MONGO_DB_NAME", DEFAULT_MONGO_DB_NAME).strip()
    return f"mongodb://{host}:{port}/{database_name}"


def get_mongo_settings(env: dict[str, str] | None = None) -> MongoSettings:
    env = os.environ if env is None else env
    default_uri = build_default_mongo_uri(env)
    uri = env.get("MONGO_URI", default_uri).strip()
    database_name = env.get("MONGO_DB_NAME", DEFAULT_MONGO_DB_NAME).strip()

    if not uri:
        raise MongoConfigError(
            "MONGO_URI is empty. Set it in HarbourSense/.env or use the local default "
            f"{default_uri!r} when MongoDB is running on the host."
        )
    if _looks_like_placeholder(uri):
        raise MongoConfigError("MONGO_URI still looks like a placeholder. Replace it in HarbourSense/.env.")
    if not database_name:
        raise MongoConfigError("MONGO_DB_NAME is empty. Set it to the target database name, for example 'port'.")
    if _looks_like_placeholder(database_name):
        raise MongoConfigError("MONGO_DB_NAME still looks like a placeholder. Replace it in HarbourSense/.env.")

    parsed = urlparse(uri)
    if parsed.scheme not in {"mongodb", "mongodb+srv"}:
        raise MongoConfigError("MONGO_URI must start with 'mongodb://' or 'mongodb+srv://'.")
    if not parsed.netloc:
        raise MongoConfigError("MONGO_URI is missing a MongoDB host.")

    return MongoSettings(
        uri=uri,
        database_name=database_name,
        server_selection_timeout_ms=_parse_timeout(env.get("MONGO_SERVER_SELECTION_TIMEOUT_MS")),
    )


def create_mongo_client(settings: MongoSettings | None = None, client_factory=None):
    settings = settings or get_mongo_settings()
    if client_factory is None:
        from motor.motor_asyncio import AsyncIOMotorClient

        client_factory = AsyncIOMotorClient

    try:
        return client_factory(settings.uri, serverSelectionTimeoutMS=settings.server_selection_timeout_ms)
    except Exception as exc:
        detail = "MongoDB client rejected MONGO_URI."
        if settings.uri.startswith("mongodb+srv://"):
            detail = (
                "MongoDB SRV lookup failed or the Atlas hostname is malformed. "
                "Check MONGO_URI in HarbourSense/.env and confirm the cluster hostname exists."
            )
        raise MongoConfigError(f"{detail} Original error type: {exc.__class__.__name__}.") from None


def get_mongo_database(settings: MongoSettings | None = None, client=None):
    settings = settings or get_mongo_settings()
    client = client or create_mongo_client(settings)
    return client[settings.database_name]
