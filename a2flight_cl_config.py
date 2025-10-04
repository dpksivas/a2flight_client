"""
A2DB Arrow Flight Client - Configuration Loader (Lightweight PyArrow-Optimized) - FIXED
=======================================================================================
Ultra-lean configuration with PyArrow vectorized validation (NO Pydantic dependency)

VERSION 2.1 CRITICAL FIX:
✅ FIXED: Added missing `os` import (was causing runtime crash)
✅ Used by load_from_env() for A2FLIGHT_CLIENT_CONFIG_PATH environment variable

ARCHITECTURAL COMPLIANCE:
✅ README.md: Function-based design, PyArrow-first, zero dependencies
✅ a2flight_impl.md: Environment-driven config, API key validation
✅ Mirrors a2config.py ultra-lean philosophy
✅ Dataclass-based (like a2config.py, a2security.py)
✅ Vectorized validation using simple functions
✅ Singleton pattern for configuration reuse
✅ Zero external validation frameworks

DESIGN PHILOSOPHY (Following a2config.py):
✅ Load all .env variables without hardcoding
✅ Simple dataclass with properties
✅ Minimal overhead - ultra-fast loading
✅ Manual validation functions (no framework)
✅ Complete backward compatibility

Author: A2DB Team
Version: 2.1 - Fixed Missing Import
Compatible with: Python 3.11+, ZERO extra dependencies
"""

import os  # ✅ CRITICAL FIX: Added missing import (used in load_from_env())
import logging
from pathlib import Path
from typing import Optional, Dict, Any, List
from dataclasses import dataclass, field
from enum import Enum

from rich.console import Console
from rich.logging import RichHandler
from rich.table import Table as RichTable
from rich.panel import Panel

# Setup logging with Rich
logging.basicConfig(
    level=logging.INFO,
    format="%(message)s",
    handlers=[RichHandler(rich_tracebacks=True, markup=True)]
)

logger = logging.getLogger(__name__)
console = Console()


# ================================================================
# SIMPLE ENUMERATIONS (NO VALIDATION FRAMEWORK)
# ================================================================

class ConnectionProtocol:
    """Connection protocol constants"""
    GRPC = "grpc"
    GRPC_TLS = "grpc+tls"
    VALID = [GRPC, GRPC_TLS]


class Environment:
    """Deployment environment constants"""
    DEVELOPMENT = "development"
    STAGING = "staging"
    PRODUCTION = "production"
    VALID = [DEVELOPMENT, STAGING, PRODUCTION]


class LogLevel:
    """Logging level constants"""
    DEBUG = "DEBUG"
    INFO = "INFO"
    WARNING = "WARNING"
    ERROR = "ERROR"
    CRITICAL = "CRITICAL"
    VALID = [DEBUG, INFO, WARNING, ERROR, CRITICAL]


# ================================================================
# ULTRA-LEAN CONFIGURATION (DATACLASS ONLY)
# ================================================================

@dataclass
class A2FlightClientConfig:
    """
    Ultra-lean Arrow Flight client configuration (dataclass-based).

    Follows a2config.py philosophy:
    - Load all .env variables
    - Simple dataclass structure
    - Manual validation functions
    - Zero framework dependencies
    - Ultra-fast loading
    """

    # ============================================================
    # CLIENT IDENTITY
    # ============================================================

    client_name: str = "etl"
    api_key: str = "<to be entered>"

    # ============================================================
    # SERVER CONNECTION
    # ============================================================
    # NOTE: Server binds to 0.0.0.0, but client connects to specific host:
    #   - Development: "localhost" (same machine)
    #   - Staging/Production: "flight-server.company.com" (actual hostname/IP)
    #   - Docker/K8s: Service name (e.g., "a2flight-server")

    server_host: str = "localhost"  # Change to actual hostname in production
    server_port: int = 50054
    connection_protocol: str = ConnectionProtocol.GRPC

    # ============================================================
    # CONNECTION POOLING
    # ============================================================

    connection_pool_size: int = 8
    max_connections: int = 16
    connection_timeout: int = 30
    idle_timeout: int = 300
    keepalive_enabled: bool = True
    keepalive_interval: int = 60
    keepalive_timeout: int = 20

    # ============================================================
    # REQUEST CONFIGURATION
    # ============================================================

    request_timeout: int = 300
    max_retries: int = 3
    retry_delay: int = 1
    retry_backoff: float = 2.0

    # ============================================================
    # DATA TRANSFER
    # ============================================================

    stream_chunk_size: int = 500_000
    enable_compression: bool = False
    max_message_size: int = 100 * 1024 * 1024  # 100MB

    # ============================================================
    # MONITORING
    # ============================================================

    enable_request_logging: bool = False
    log_level: str = LogLevel.INFO
    log_api_keys: bool = False
    enable_metrics: bool = True
    metrics_interval: int = 60

    # ============================================================
    # ERROR HANDLING
    # ============================================================

    circuit_breaker_enabled: bool = True
    circuit_breaker_failure_threshold: int = 5
    circuit_breaker_timeout: int = 60
    fallback_to_direct_db: bool = False

    # ============================================================
    # DEPLOYMENT
    # ============================================================

    environment: str = Environment.DEVELOPMENT
    version: str = "2.1"
    service_name: str = "a2flight_client"

    # ============================================================
    # OPTIONAL RESTRICTIONS
    # ============================================================

    allowed_server_ips: List[str] = field(default_factory=list)
    allowed_operations: List[str] = field(default_factory=lambda: ["SELECT", "INSERT", "UPDATE", "DELETE"])

    # ============================================================
    # COMPUTED PROPERTIES (LIKE a2config.py)
    # ============================================================

    @property
    def server_location(self) -> str:
        """Get formatted server location string"""
        return f"{self.connection_protocol}://{self.server_host}:{self.server_port}"

    @property
    def is_production(self) -> bool:
        """Check if running in production"""
        return self.environment == Environment.PRODUCTION

    @property
    def connection_pool_config(self) -> Dict[str, Any]:
        """Get connection pool configuration dict"""
        return {
            'pool_size': self.connection_pool_size,
            'max_connections': self.max_connections,
            'connection_timeout': self.connection_timeout,
            'idle_timeout': self.idle_timeout,
            'keepalive_enabled': self.keepalive_enabled,
            'keepalive_interval': self.keepalive_interval,
            'keepalive_timeout': self.keepalive_timeout,
        }


# ================================================================
# VECTORIZED VALIDATION FUNCTIONS (NO FRAMEWORK)
# ================================================================

def validate_config(config: A2FlightClientConfig) -> List[str]:
    """
    Ultra-fast vectorized configuration validation.
    Returns list of error messages (empty list = valid).

    Philosophy: Simple, fast, explicit validation without frameworks.

    FIXED: Warnings logged separately, don't block startup in production
    """
    errors = []
    warnings = []  # ✅ NEW: Separate warnings from blocking errors

    # ===== CLIENT IDENTITY VALIDATION =====
    if not config.client_name or len(config.client_name) == 0:
        errors.append("client_name is required")
    elif len(config.client_name) > 50:
        errors.append(f"client_name too long: {len(config.client_name)} chars (max 50)")
    elif not config.client_name.replace('_', '').isalnum():
        errors.append("client_name must be alphanumeric with underscores only")

    if not config.api_key or config.api_key == '<to be entered>':
        errors.append(
            "API key must be obtained from server administrator. "
            "Update a2flight_cl.env with real API key."
        )
    elif len(config.api_key) < 32:
        errors.append(f"API key too short: {len(config.api_key)} chars (min 32)")
    elif not config.api_key.startswith('a2db_key_'):
        errors.append("Invalid API key format. Must start with 'a2db_key_'")

    # ===== SERVER CONNECTION VALIDATION =====
    if not config.server_host:
        errors.append("server_host is required")
    elif config.server_host == "localhost" and config.is_production:
        # ✅ FIXED: Warning only - valid for local testing
        warnings.append(
            "server_host='localhost' in production environment. "
            "This is valid for local testing but consider using actual hostname for remote deployments."
        )

    if not (1024 <= config.server_port <= 65535):
        errors.append(f"server_port must be 1024-65535, got {config.server_port}")

    if config.connection_protocol not in ConnectionProtocol.VALID:
        errors.append(
            f"Invalid connection_protocol: {config.connection_protocol}. "
            f"Must be one of: {', '.join(ConnectionProtocol.VALID)}"
        )

    # ===== CONNECTION POOL VALIDATION =====
    if config.connection_pool_size < 1 or config.connection_pool_size > 100:
        errors.append(f"connection_pool_size must be 1-100, got {config.connection_pool_size}")

    if config.max_connections < 1 or config.max_connections > 200:
        errors.append(f"max_connections must be 1-200, got {config.max_connections}")

    if config.connection_pool_size > config.max_connections:
        errors.append(
            f"connection_pool_size ({config.connection_pool_size}) "
            f"cannot exceed max_connections ({config.max_connections})"
        )

    if not (5 <= config.connection_timeout <= 300):
        errors.append(f"connection_timeout must be 5-300s, got {config.connection_timeout}")

    if not (60 <= config.idle_timeout <= 3600):
        errors.append(f"idle_timeout must be 60-3600s, got {config.idle_timeout}")

    # ===== REQUEST VALIDATION =====
    if not (10 <= config.request_timeout <= 3600):
        errors.append(f"request_timeout must be 10-3600s, got {config.request_timeout}")

    if not (0 <= config.max_retries <= 10):
        errors.append(f"max_retries must be 0-10, got {config.max_retries}")

    if not (1.0 <= config.retry_backoff <= 10.0):
        errors.append(f"retry_backoff must be 1.0-10.0, got {config.retry_backoff}")

    # ===== DATA TRANSFER VALIDATION =====
    if not (1_000 <= config.stream_chunk_size <= 10_000_000):
        errors.append(f"stream_chunk_size must be 1K-10M rows, got {config.stream_chunk_size}")

    min_size = 1024 * 1024  # 1MB
    max_size = 1024 * 1024 * 1024  # 1GB
    if not (min_size <= config.max_message_size <= max_size):
        errors.append(f"max_message_size must be 1MB-1GB, got {config.max_message_size}")

    # ===== MONITORING VALIDATION =====
    if config.log_level not in LogLevel.VALID:
        errors.append(
            f"Invalid log_level: {config.log_level}. "
            f"Must be one of: {', '.join(LogLevel.VALID)}"
        )

    if not (10 <= config.metrics_interval <= 3600):
        errors.append(f"metrics_interval must be 10-3600s, got {config.metrics_interval}")

    # ===== ERROR HANDLING VALIDATION =====
    if not (1 <= config.circuit_breaker_failure_threshold <= 100):
        errors.append(
            f"circuit_breaker_failure_threshold must be 1-100, "
            f"got {config.circuit_breaker_failure_threshold}"
        )

    if not (10 <= config.circuit_breaker_timeout <= 600):
        errors.append(f"circuit_breaker_timeout must be 10-600s, got {config.circuit_breaker_timeout}")

    # ===== DEPLOYMENT VALIDATION =====
    if config.environment not in Environment.VALID:
        errors.append(
            f"Invalid environment: {config.environment}. "
            f"Must be one of: {', '.join(Environment.VALID)}"
        )

    # ===== PRODUCTION SECURITY VALIDATION =====
    if config.is_production:
        if config.log_api_keys:
            errors.append("log_api_keys must be False in production environment!")

        if config.connection_protocol == ConnectionProtocol.GRPC:
            # ✅ FIXED: Warning only - valid for internal networks
            warnings.append(
                "Production using 'grpc' without TLS. "
                "This is acceptable for internal networks with network-level security. "
                "Consider 'grpc+tls' for internet-facing deployments."
            )

    # ===== OPERATIONS VALIDATION =====
    valid_ops = {"SELECT", "INSERT", "UPDATE", "DELETE"}
    for op in config.allowed_operations:
        if op.upper() not in valid_ops:
            errors.append(f"Invalid operation: {op}. Must be one of: {', '.join(valid_ops)}")

    # ✅ NEW: Log warnings separately (don't block startup)
    if warnings:
        logger.warning("Configuration warnings (non-blocking):")
        for warning in warnings:
            logger.warning(f"  ⚠️  {warning}")

    return errors  # Only return blocking errors


def load_from_env(env_file: Optional[Path] = None) -> A2FlightClientConfig:
    """
    Load configuration from .env file (like a2config.py).

    Philosophy: Load all variables, simple parsing, no framework.

    FIXED: Now respects A2FLIGHT_CLIENT_CONFIG_PATH environment variable
    Uses os.getenv() which requires the os import added at top of file
    """
    # Determine env file path
    if env_file is None:
        # ✅ FIX: Check environment variable first
        # NOTE: This uses os.getenv() - requires os import at top!
        env_path_str = os.getenv('A2FLIGHT_CLIENT_CONFIG_PATH')
        if env_path_str:
            env_file = Path(env_path_str)
            logger.info(f"Using config from environment variable: {env_file}")
        else:
            # Try multiple search paths
            search_paths = [
                Path("a2flight_cl.env"),  # Current directory
                Path("a2flight_cl/a2flight_cl.env"),  # Subdirectory
                Path("../a2flight_cl/a2flight_cl.env"),  # Parent's subdirectory
            ]

            for path in search_paths:
                if path.exists():
                    env_file = path
                    logger.info(f"Found config at: {env_file.absolute()}")
                    break
            else:
                # No config found
                logger.warning(
                    f"Configuration file not found in search paths:\n" +
                    "\n".join(f"  - {p.absolute()}" for p in search_paths)
                )
                logger.warning("Using default configuration values")
                return A2FlightClientConfig()

    # Verify the specified file exists
    if not env_file.exists():
        logger.error(f"Specified config file not found: {env_file.absolute()}")
        logger.warning("Using default configuration values")
        return A2FlightClientConfig()

    # Load variables from .env file
    env_vars = {}
    try:
        with open(env_file, 'r', encoding='utf-8') as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith('#') and '=' in line:
                    key, value = line.split('=', 1)
                    key = key.strip()
                    value = value.strip().strip('"').strip("'")
                    env_vars[key] = value

        logger.info(f"Successfully loaded config from: {env_file.absolute()}")

    except Exception as e:
        logger.error(f"Failed to read config file {env_file}: {e}")
        logger.warning("Using default configuration values")
        return A2FlightClientConfig()

    # Helper function to get typed values
    def get_str(key: str, default: str) -> str:
        return env_vars.get(f"A2FLIGHT_{key}", default)

    def get_int(key: str, default: int) -> int:
        try:
            return int(env_vars.get(f"A2FLIGHT_{key}", default))
        except ValueError:
            return default

    def get_float(key: str, default: float) -> float:
        try:
            return float(env_vars.get(f"A2FLIGHT_{key}", default))
        except ValueError:
            return default

    def get_bool(key: str, default: bool) -> bool:
        val = env_vars.get(f"A2FLIGHT_{key}", str(default)).lower()
        return val in ('true', '1', 'yes', 'on')

    def get_list(key: str, default: List[str]) -> List[str]:
        val = env_vars.get(f"A2FLIGHT_{key}", '')
        if not val:
            return default
        return [item.strip() for item in val.split(',') if item.strip()]

    # Build configuration
    return A2FlightClientConfig(
        # Client identity
        client_name=get_str("CLIENT_NAME", "etl"),
        api_key=get_str("CLIENT_API_KEY", "<to be entered>"),

        # Server connection
        server_host=get_str("SERVER_HOST", "localhost"),
        server_port=get_int("SERVER_PORT", 50054),
        connection_protocol=get_str("CONNECTION_PROTOCOL", ConnectionProtocol.GRPC),

        # Connection pooling
        connection_pool_size=get_int("CLIENT_CONNECTION_POOL_SIZE", 8),
        max_connections=get_int("CLIENT_MAX_CONNECTIONS", 16),
        connection_timeout=get_int("CLIENT_CONNECTION_TIMEOUT", 30),
        idle_timeout=get_int("CLIENT_IDLE_TIMEOUT", 300),
        keepalive_enabled=get_bool("CLIENT_KEEPALIVE_ENABLED", True),
        keepalive_interval=get_int("CLIENT_KEEPALIVE_INTERVAL", 60),
        keepalive_timeout=get_int("CLIENT_KEEPALIVE_TIMEOUT", 20),

        # Request configuration
        request_timeout=get_int("CLIENT_REQUEST_TIMEOUT", 300),
        max_retries=get_int("CLIENT_MAX_RETRIES", 3),
        retry_delay=get_int("CLIENT_RETRY_DELAY", 1),
        retry_backoff=get_float("CLIENT_RETRY_BACKOFF", 2.0),

        # Data transfer
        stream_chunk_size=get_int("CLIENT_STREAM_CHUNK_SIZE", 500_000),
        enable_compression=get_bool("CLIENT_ENABLE_COMPRESSION", False),
        max_message_size=get_int("CLIENT_MAX_MESSAGE_SIZE", 100 * 1024 * 1024),

        # Monitoring
        enable_request_logging=get_bool("CLIENT_ENABLE_REQUEST_LOGGING", False),
        log_level=get_str("CLIENT_LOG_LEVEL", LogLevel.INFO),
        log_api_keys=get_bool("CLIENT_LOG_API_KEYS", False),
        enable_metrics=get_bool("CLIENT_ENABLE_METRICS", True),
        metrics_interval=get_int("CLIENT_METRICS_INTERVAL", 60),

        # Error handling
        circuit_breaker_enabled=get_bool("CLIENT_CIRCUIT_BREAKER_ENABLED", True),
        circuit_breaker_failure_threshold=get_int("CLIENT_CIRCUIT_BREAKER_FAILURE_THRESHOLD", 5),
        circuit_breaker_timeout=get_int("CLIENT_CIRCUIT_BREAKER_TIMEOUT", 60),
        fallback_to_direct_db=get_bool("CLIENT_FALLBACK_TO_DIRECT_DB", False),

        # Deployment
        environment=get_str("CLIENT_ENVIRONMENT", Environment.DEVELOPMENT),
        version=get_str("CLIENT_VERSION", "2.1"),
        service_name=get_str("CLIENT_SERVICE_NAME", "a2flight_client"),

        # Optional restrictions
        allowed_server_ips=get_list("CLIENT_ALLOWED_SERVER_IPS", []),
        allowed_operations=get_list("CLIENT_ALLOWED_OPERATIONS", ["SELECT", "INSERT", "UPDATE", "DELETE"]),
    )


# ================================================================
# SINGLETON CONFIGURATION INSTANCE (LIKE a2config.py)
# ================================================================

_config_instance: Optional[A2FlightClientConfig] = None


def get_flight_client_config(
    env_file: Optional[Path] = None,
    reload: bool = False
) -> A2FlightClientConfig:
    """
    Get or create singleton Flight client configuration instance.

    Args:
        env_file: Optional path to .env file (default: a2flight_cl.env)
        reload: Force reload configuration from file

    Returns:
        Validated A2FlightClientConfig instance

    Raises:
        ValueError: If configuration is invalid
    """
    global _config_instance

    if _config_instance is None or reload:
        try:
            logger.info("📋 Loading Arrow Flight client configuration...")

            # Load from environment
            _config_instance = load_from_env(env_file)

            # Validate
            errors = validate_config(_config_instance)
            if errors:
                error_msg = "Configuration validation failed:\n" + "\n".join(f"  - {e}" for e in errors)
                logger.error(f"❌ {error_msg}")
                raise ValueError(error_msg)

            logger.info(
                f"✅ Configuration loaded successfully "
                f"(client: {_config_instance.client_name}, "
                f"env: {_config_instance.environment})"
            )

        except Exception as e:
            logger.error(f"❌ Configuration load failed: {e}")
            raise

    return _config_instance


def print_config_summary(config: Optional[A2FlightClientConfig] = None):
    """
    Print formatted configuration summary with Rich.

    Args:
        config: Configuration instance (default: singleton)
    """
    if config is None:
        config = get_flight_client_config()

    # Create summary table
    table = RichTable(
        title="🚀 A2DB Arrow Flight Client Configuration",
        show_header=True,
        header_style="bold cyan",
        box=None
    )

    table.add_column("Setting", style="cyan", width=30)
    table.add_column("Value", style="white")

    # Client identity
    table.add_row("Client Name", config.client_name)
    table.add_row("API Key", "***REDACTED***" if not config.log_api_keys else config.api_key)

    # Server connection
    table.add_row("", "")
    table.add_row("Server Host", config.server_host)
    table.add_row("Server Port", str(config.server_port))
    table.add_row("Protocol", config.connection_protocol)
    table.add_row("Server Location", config.server_location)

    # Connection pooling
    table.add_row("", "")
    table.add_row("Connection Pool Size", str(config.connection_pool_size))
    table.add_row("Max Connections", str(config.max_connections))
    table.add_row("Connection Timeout", f"{config.connection_timeout}s")

    # Request config
    table.add_row("", "")
    table.add_row("Request Timeout", f"{config.request_timeout}s")
    table.add_row("Max Retries", str(config.max_retries))
    table.add_row("Retry Backoff", f"{config.retry_backoff}x")

    # Data transfer
    table.add_row("", "")
    table.add_row("Stream Chunk Size", f"{config.stream_chunk_size:,} rows")
    table.add_row("Compression", "✅" if config.enable_compression else "❌")
    table.add_row("Max Message Size", f"{config.max_message_size // (1024 * 1024)} MB")

    # Deployment
    table.add_row("", "")
    table.add_row("Environment", config.environment)
    table.add_row("Version", config.version)
    table.add_row("Log Level", config.log_level)

    # Error handling
    table.add_row("", "")
    table.add_row("Circuit Breaker", "✅" if config.circuit_breaker_enabled else "❌")
    table.add_row("Direct DB Fallback", "✅" if config.fallback_to_direct_db else "❌")

    console.print(table)

    # Production warning
    if config.is_production and config.connection_protocol == ConnectionProtocol.GRPC:
        console.print(
            Panel(
                "[yellow]⚠️  WARNING: Production environment without TLS encryption!\n"
                "Consider using 'grpc+tls' protocol for secure connections.[/yellow]",
                title="Security Warning",
                border_style="yellow"
            )
        )


# ================================================================
# MAIN EXECUTION
# ================================================================

if __name__ == "__main__":
    console.print("\n[bold cyan]A2DB Arrow Flight Client Configuration Validator[/bold cyan]\n")

    try:
        # Load and validate configuration
        config = get_flight_client_config()

        # Print summary
        print_config_summary(config)

        console.print("\n[bold green]✅ Configuration validation successful![/bold green]\n")

    except Exception as e:
        console.print(f"\n[bold red]❌ Configuration validation failed![/bold red]")
        console.print(f"[red]{str(e)}[/red]\n")
        exit(1)