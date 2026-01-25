"""
Configuration management for Databricks MCP services
"""

from typing import Dict, Any, Optional


class ServiceConfig:
    """Configuration for individual services."""

    def __init__(self, enabled: bool = True, **kwargs: Any) -> None:
        self.enabled = enabled
        self.options = kwargs


class DatabricksConfig:
    """Main configuration for Databricks MCP server."""

    def __init__(self) -> None:
        self.sql_service = ServiceConfig(enabled=True)
        self.uc_service = ServiceConfig(enabled=True)
        self.ws_service = ServiceConfig(enabled=True)
        self.jobs_service = ServiceConfig(enabled=True)

    def is_service_enabled(self, service_name: str) -> bool:
        """Check if a service is enabled."""
        service_config = getattr(self, f"{service_name}_service", None)
        return service_config.enabled if service_config else False

    def get_service_config(self, service_name: str) -> Optional[ServiceConfig]:
        """Get configuration for a specific service."""
        return getattr(self, f"{service_name}_service", None)

    def disable_service(self, service_name: str) -> None:
        """Disable a specific service."""
        service_config = getattr(self, f"{service_name}_service", None)
        if service_config:
            service_config.enabled = False

    def enable_service(self, service_name: str) -> None:
        """Enable a specific service."""
        service_config = getattr(self, f"{service_name}_service", None)
        if service_config:
            service_config.enabled = True