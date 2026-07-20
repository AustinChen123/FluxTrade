import os
from dataclasses import dataclass


ENVIRONMENT_VARIABLE = "FLUXTRADE_ENVIRONMENT"


def validate_environment_identity(value: str) -> str:
    identity = value
    if (
        not identity
        or not identity[0].isalnum()
        or not identity[-1].isalnum()
        or not all(char.islower() or char.isdigit() or char == "-" for char in identity)
    ):
        raise ValueError(
            f"{ENVIRONMENT_VARIABLE} must contain lowercase ASCII letters, digits, "
            "or internal hyphens"
        )
    if not identity.isascii():
        raise ValueError(f"{ENVIRONMENT_VARIABLE} must be ASCII")
    return identity


@dataclass(frozen=True)
class RuntimeEnvironment:
    identity: str

    @classmethod
    def from_env(cls) -> "RuntimeEnvironment":
        identity = os.getenv(ENVIRONMENT_VARIABLE)
        if identity is None:
            raise ValueError(f"{ENVIRONMENT_VARIABLE} must be set explicitly")
        return cls(validate_environment_identity(identity))

    def key(self, suffix: str) -> str:
        return f"fluxtrade:{self.identity}:{suffix}"

    @property
    def allows_external_kill(self) -> bool:
        return self.identity == "live"
