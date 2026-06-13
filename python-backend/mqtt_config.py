import os
import ssl
from dataclasses import dataclass


DEFAULT_MQTT_MODE = "local"
DEFAULT_MQTT_HOST = "localhost"
DEFAULT_MQTT_PORT = 1883
DEFAULT_AWS_MQTT_PORT = 8883


class MqttConfigError(RuntimeError):
    """Raised when MQTT runtime configuration is missing or invalid."""


@dataclass(frozen=True)
class MqttSettings:
    mode: str
    host: str
    port: int
    ca_path: str | None = None
    cert_path: str | None = None
    key_path: str | None = None


def _parse_port(value: str | None, default: int) -> int:
    if not value:
        return default
    try:
        port = int(value)
    except ValueError as exc:
        raise MqttConfigError("MQTT port values must be integers.") from exc
    if port <= 0:
        raise MqttConfigError("MQTT port values must be greater than zero.")
    return port


def _looks_like_placeholder(value: str) -> bool:
    lowered = value.lower()
    return any(marker in lowered for marker in ("<", ">", "your-", "example.com"))


def get_mqtt_settings(env: dict[str, str] | None = None) -> MqttSettings:
    env = os.environ if env is None else env
    mode = env.get("MQTT_MODE", DEFAULT_MQTT_MODE).strip().lower()

    if mode == "local":
        return MqttSettings(
            mode=mode,
            host=env.get("MQTT_BROKER_HOST", DEFAULT_MQTT_HOST).strip(),
            port=_parse_port(env.get("MQTT_BROKER_PORT"), DEFAULT_MQTT_PORT),
        )

    if mode != "aws":
        raise MqttConfigError("Unsupported MQTT_MODE. Use 'local' or 'aws'.")

    endpoint = env.get("AWS_IOT_ENDPOINT", "").strip()
    ca_path = env.get("AWS_IOT_CA_PATH", "").strip()
    cert_path = env.get("AWS_IOT_CERT_PATH", "").strip()
    key_path = env.get("AWS_IOT_KEY_PATH", "").strip()
    missing = [
        name
        for name, value in (
            ("AWS_IOT_ENDPOINT", endpoint),
            ("AWS_IOT_CA_PATH", ca_path),
            ("AWS_IOT_CERT_PATH", cert_path),
            ("AWS_IOT_KEY_PATH", key_path),
        )
        if not value or _looks_like_placeholder(value)
    ]
    if missing:
        raise MqttConfigError(
            "MQTT_MODE=aws requires real AWS IoT endpoint and certificate paths in HarbourSense/.env: "
            + ", ".join(missing)
        )

    return MqttSettings(
        mode=mode,
        host=endpoint,
        port=_parse_port(env.get("AWS_IOT_PORT"), DEFAULT_AWS_MQTT_PORT),
        ca_path=ca_path,
        cert_path=cert_path,
        key_path=key_path,
    )


def build_tls_context(settings: MqttSettings) -> ssl.SSLContext | None:
    if settings.mode != "aws":
        return None
    tls_context = ssl.create_default_context(ssl.Purpose.SERVER_AUTH)
    tls_context.load_verify_locations(cafile=settings.ca_path)
    tls_context.load_cert_chain(certfile=settings.cert_path, keyfile=settings.key_path)
    return tls_context


def build_aiomqtt_params(identifier: str, env: dict[str, str] | None = None) -> dict:
    settings = get_mqtt_settings(env)
    params = {
        "hostname": settings.host,
        "port": settings.port,
        "identifier": identifier,
    }
    tls_context = build_tls_context(settings)
    if tls_context is not None:
        params["tls_context"] = tls_context
    return params


def configure_paho_client(client, env: dict[str, str] | None = None) -> MqttSettings:
    settings = get_mqtt_settings(env)
    if settings.mode == "aws":
        client.tls_set(
            ca_certs=settings.ca_path,
            certfile=settings.cert_path,
            keyfile=settings.key_path,
        )
    return settings
