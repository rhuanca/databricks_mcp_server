"""
Common authentication utilities for Databricks services
"""

import os
from typing import Dict, Tuple


# Environment variable names
DATABRICKS_HOST_ENV = "DATABRICKS_HOST"
DATABRICKS_TOKEN_ENV = "DATABRICKS_TOKEN"


def get_databricks_credentials() -> Tuple[str, str]:
    """
    Get Databricks host and token from environment variables.

    Returns:
        Tuple of (host, token)

    Raises:
        EnvironmentError: If required environment variables are not set
    """
    host = os.getenv(DATABRICKS_HOST_ENV)
    token = os.getenv(DATABRICKS_TOKEN_ENV)

    if not host:
        raise EnvironmentError(f"Environment variable {DATABRICKS_HOST_ENV} must be set.")
    if not token:
        raise EnvironmentError(f"Environment variable {DATABRICKS_TOKEN_ENV} must be set.")

    return host, token


def get_databricks_headers() -> Dict[str, str]:
    """
    Get standard Databricks API headers with authentication.

    Returns:
        Headers dictionary with Authorization
    """
    _, token = get_databricks_credentials()
    return {"Authorization": f"Bearer {token}"}


def get_databricks_base_url() -> str:
    """
    Get Databricks base URL for API calls.

    Returns:
        Base URL (e.g., https://your-workspace.databricks.com)
    """
    host, _ = get_databricks_credentials()
    return f"https://{host}"