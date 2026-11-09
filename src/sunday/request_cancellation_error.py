# Copyright 2026 Outfox, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.


class RequestCancellationError(Exception):
    """Stop this request without wrapping, retrying, or reconnecting.

    Subclass this exception (optionally alongside a framework exception) to veto
    further work. Ordinary errors still follow the transport's normal policy.
    This does not cancel the caller's asyncio task.
    """
