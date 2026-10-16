from dataclasses import dataclass
from os import getenv
from typing import Self

from dotenv import load_dotenv


def _get_from_env(name: str) -> str:
    value = getenv(name)
    if not value:
        raise ValueError(f"Переменная окружения '{name}' должна быть установлена.")
    return value


@dataclass(frozen=True)
class Config:
    DATABASE_URL: str
    RABBITMQ_URL: str
    API_KEY: str

    @classmethod
    def get_config(cls) -> Self:
        load_dotenv(".env")
        return cls(
            DATABASE_URL=_get_from_env("DATABASE_URL"),
            RABBITMQ_URL=_get_from_env("RABBITMQ_URL"),
            API_KEY=_get_from_env("API_KEY"),
        )


CONFIG = Config.get_config()
