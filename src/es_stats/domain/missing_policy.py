from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum


class MissingPolicyMode(StrEnum):
    STRICT = "STRICT"
    ALLOW_MISSING_UP_TO = "ALLOW_MISSING_UP_TO"


class WindowRole(StrEnum):
    X = "X"
    Y = "Y"


@dataclass(frozen=True)
class MissingPolicy:
    """
    Missing-data policy with independent tolerance for X and Y windows.

    Tolerances are fractions in [0.0, 1.0], e.g. 0.10 allows up to 10% missing.
    """

    mode: MissingPolicyMode
    x_tol: float = 0.0
    y_tol: float = 0.0

    def __post_init__(self) -> None:
        for label, value in (("x_tol", self.x_tol), ("y_tol", self.y_tol)):
            if value < 0.0 or value > 1.0:
                raise ValueError(f"{label} must be in [0.0, 1.0], got {value!r}")

        if self.mode == MissingPolicyMode.STRICT and (self.x_tol != 0.0 or self.y_tol != 0.0):
            raise ValueError("STRICT mode requires x_tol=0.0 and y_tol=0.0")

    def tolerance_for(self, role: WindowRole) -> float:
        if self.mode == MissingPolicyMode.STRICT:
            return 0.0
        if role == WindowRole.X:
            return self.x_tol
        if role == WindowRole.Y:
            return self.y_tol
        raise ValueError(f"Unsupported window role: {role!r}")
