"""dv360 Authentication."""

from __future__ import annotations
from singer_sdk.authenticators import OAuthAuthenticator
from typing import Dict


class dv360Authenticator(OAuthAuthenticator):
    """Authenticator class for dv360 using OAuth 2.0."""

    @property
    def oauth_request_body(self) -> dict:
        """Define the OAuth request body for the DV360 API.

        Returns:
            A dict with the request body.
        """
        return {
            "grant_type": "refresh_token",
            "client_id": self.client_id,
            "client_secret": self.client_secret,
            "refresh_token": self.config["refresh_token"],
        }

    @classmethod
    def create_for_stream(cls, stream) -> dv360Authenticator:
        """Instantiate an authenticator for a specific Singer stream.

        Args:
            stream: The Singer stream instance.

        Returns:
            A new authenticator instance.
        """
        return cls(
            stream=stream,
            auth_endpoint="https://oauth2.googleapis.com/token",
            oauth_scopes="https://www.googleapis.com/auth/doubleclickbidmanager",
        )
