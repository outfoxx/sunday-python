# Copyright 2026 Outfox, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.

"""Compatibility import for the HTTPX event stream implementation."""

from .httpx import HttpxEventStream

__all__ = ["HttpxEventStream"]
