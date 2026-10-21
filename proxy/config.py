import json


def load_config(config_file_path=None):
    server_config = {
        "listen": "127.0.0.1:8080",
        "upstreams": [
            {"host": "127.0.0.1", "port": 9001},
            {"host": "127.0.0.1", "port": 9002},
        ],
        "timeouts": {
            "connect_ms": 1000,
            "read_ms": 15000,
            "write_ms": 15000,
            "total_ms": 30000,
        },
        "limits": {"max_client_conns": 1000, "max_conns_per_upstream": 100},
        "logging": {"level": "info"},
    }
    if config_file_path:
        with open(config_file_path, encoding="utf-8") as config_file:
            configured_settings = json.load(config_file)
        for setting_name, setting_value in configured_settings.items():
            if setting_name in ("timeouts", "limits", "logging"):
                server_config[setting_name].update(setting_value)
            else:
                server_config[setting_name] = setting_value

    if not server_config["upstreams"]:
        raise ValueError("Нужен хотя бы один upstream")
    for connection_limit in server_config["limits"].values():
        if connection_limit <= 0:
            raise ValueError("Лимиты должны быть положительными")
    for timeout_ms in server_config["timeouts"].values():
        if timeout_ms <= 0:
            raise ValueError("Таймауты должны быть положительными")
    return server_config
