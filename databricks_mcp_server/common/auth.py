"""
Common authentication utilities for Databricks services
"""
import os
from typing import Dict, Tuple


def get_databricks_credentials() -> Tuple[str, str]:
    """
    Get Databricks host and token from environment variables.
    
    Returns:
        Tuple[str, str]: (host, token)
    
    Raises:
        EnvironmentError: If required environment variables are not set
    """
    host = os.getenv("DATABRICKS_HOST")
    token = os.getenv("DATABRICKS_TOKEN")
    
    if not host or not token:
        raise EnvironmentError(
            "Environment variables DATABRICKS_HOST and DATABRICKS_TOKEN must be set."
        )
    
    return host, token


def get_databricks_headers() -> Dict[str, str]:
    """
    Get standard Databricks API headers with authentication.
    
    Returns:
        Dict[str, str]: Headers dictionary with Authorization
    """
    _, token = get_databricks_credentials()
    return {"Authorization": f"Bearer {token}"}


def get_databricks_base_url() -> str:
    """
    Get Databricks base URL for API calls.
    
    Returns:
        str: Base URL (e.g., https://your-workspace.databricks.com)
    """
    host, _ = get_databricks_credentials()
    return f"https://{host}"