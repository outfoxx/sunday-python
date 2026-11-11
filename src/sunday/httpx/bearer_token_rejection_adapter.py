"""Optional terminal policy for explicit bearer rejection, independent of credential ownership."""

import httpx

from ..request_cancellation_error import RequestCancellationError
from ._security import _bearer_token, _invalid_bearer_challenge
from .response_adapter import ResponseAdapter


class BearerTokenRejectionAdapter(ResponseAdapter):
    """Stop a rejected bearer exchange without refreshing, replaying, or reading response data.

    Compose after provider rejection feedback when both adapters are installed.
    """

    async def adapt(self, response: httpx.Response, request: httpx.Request) -> httpx.Response:
        """Return unrelated responses unchanged; cancellation leaves renewal to the credential owner."""
        if (
            response.status_code == 401
            and _bearer_token(request) is not None
            and _invalid_bearer_challenge(response.headers.get("www-authenticate", ""))
        ):
            raise RequestCancellationError("Access token rejected")
        return response
