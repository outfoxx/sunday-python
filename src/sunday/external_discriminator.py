"""Presence-preserving input for native external-discriminator field adapters."""

from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True, slots=True)
class ExternalDiscriminatorValue:
    """Carry an unvalidated sibling discriminator into a Pydantic field callback.

    This contains no cached validation result. The selected native schema validates
    ``value`` each time the enclosing model is checked.
    """

    discriminator: Any
    value: Any
