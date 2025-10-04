"""
A2DB Arrow Flight Client - Configuration Generator (FIXED)
==========================================================
FIXED: max_message_size now generates valid value (100MB default)

Author: A2DB Team
Version: 1.1 - Fixed max_message_size validation
"""

import os
import time
from pathlib import Path


def log(message: str):
    """Log with timestamp"""
    timestamp = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())
    print(f"[{timestamp}] {message}")


def detect_system_specs():
    """Detect system specifications for optimal configuration"""
    try:
        cpu_cores = os.cpu_count() or 4
        log(f"Detected system configuration:")
        log(f"  CPU Cores: {cpu_cores}")
        return {'cpu_cores': cpu_cores}
    except Exception as e:
        log(f"Warning: Could not detect system specs: {e}")
        return {'cpu_cores': 4}


def generate_client_config(specs):
    """Generate client configuration based on system specs"""
    cpu_cores = specs['cpu_cores']

    # Client connection pool sizing
    connection_pool_size = max(4, min(cpu_cores, 8))
    max_connections = connection_pool_size * 2

    return {
        'CONNECTION_POOL_SIZE': connection_pool_size,
        'MAX_CONNECTIONS': max_connections,
        'REQUEST_TIMEOUT': 300,
        'STREAM_CHUNK_SIZE': 500000,
        'MAX_MESSAGE_SIZE': 100 * 1024 * 1024,  # ✅ FIXED: 100MB default
    }


def write_client_env_file(config, specs):
    """Write a2flight_cl.env configuration file"""
    script_dir = Path(__file__).parent
    env_path = script_dir / "a2flight_cl.env"

    # Backup existing file
    if env_path.exists():
        timestamp = time.strftime("%d%m%y_%H%M", time.localtime())
        backup_path = script_dir / f"a2flight_cl_{timestamp}.env"
        try:
            env_path.rename(backup_path)
            log(f"Existing config backed up to: {backup_path.name}")
        except Exception as e:
            log(f"Warning: Could not backup existing file: {e}")

    current_time = time.strftime('%Y-%m-%d %H:%M:%S UTC', time.gmtime())

    content = f"""# ============================================================================
# A2DB Arrow Flight Client Configuration
# ============================================================================
# Generated: {current_time}
# CPU Cores: {specs['cpu_cores']}
# Purpose: Client-side configuration for Arrow Flight connections
# ============================================================================

# ============================================================================
# CLIENT IDENTITY
# ============================================================================
A2FLIGHT_CLIENT_NAME=etl
A2FLIGHT_CLIENT_API_KEY=<to be entered>

# ============================================================================
# SERVER CONNECTION
# ============================================================================
A2FLIGHT_SERVER_HOST=localhost
A2FLIGHT_SERVER_PORT=50054
A2FLIGHT_CONNECTION_PROTOCOL=grpc

# ============================================================================
# TLS CONFIGURATION
# ============================================================================
A2FLIGHT_CLIENT_ENABLE_TLS=false
A2FLIGHT_CLIENT_VERIFY_TLS=true

# ============================================================================
# CONNECTION POOLING
# ============================================================================
A2FLIGHT_CLIENT_CONNECTION_POOL_SIZE={config['CONNECTION_POOL_SIZE']}
A2FLIGHT_CLIENT_MAX_CONNECTIONS={config['MAX_CONNECTIONS']}
A2FLIGHT_CLIENT_CONNECTION_TIMEOUT=30
A2FLIGHT_CLIENT_IDLE_TIMEOUT=300

A2FLIGHT_CLIENT_KEEPALIVE_ENABLED=true
A2FLIGHT_CLIENT_KEEPALIVE_INTERVAL=30
A2FLIGHT_CLIENT_KEEPALIVE_TIMEOUT=60

# ============================================================================
# REQUEST CONFIGURATION
# ============================================================================
A2FLIGHT_CLIENT_REQUEST_TIMEOUT={config['REQUEST_TIMEOUT']}
A2FLIGHT_CLIENT_MAX_RETRIES=3
A2FLIGHT_CLIENT_RETRY_DELAY=1
A2FLIGHT_CLIENT_RETRY_BACKOFF=2.0

# ============================================================================
# DATA TRANSFER SETTINGS
# ============================================================================
A2FLIGHT_CLIENT_STREAM_CHUNK_SIZE={config['STREAM_CHUNK_SIZE']}
A2FLIGHT_CLIENT_ENABLE_COMPRESSION=false

# FIXED: Valid value between 1MB-1GB (100MB = 104857600 bytes)
# Production environments should use explicit size limits
A2FLIGHT_CLIENT_MAX_MESSAGE_SIZE={config['MAX_MESSAGE_SIZE']}

# ============================================================================
# MONITORING AND LOGGING
# ============================================================================
A2FLIGHT_CLIENT_ENABLE_REQUEST_LOGGING=true
A2FLIGHT_CLIENT_LOG_LEVEL=INFO
A2FLIGHT_CLIENT_LOG_API_KEYS=false

A2FLIGHT_CLIENT_ENABLE_METRICS=true
A2FLIGHT_CLIENT_METRICS_INTERVAL=60

# ============================================================================
# ERROR HANDLING
# ============================================================================
A2FLIGHT_CLIENT_CIRCUIT_BREAKER_ENABLED=true
A2FLIGHT_CLIENT_CIRCUIT_BREAKER_FAILURE_THRESHOLD=5
A2FLIGHT_CLIENT_CIRCUIT_BREAKER_TIMEOUT=60

A2FLIGHT_CLIENT_FALLBACK_TO_DIRECT_DB=false

# ============================================================================
# DEPLOYMENT METADATA
# ============================================================================
A2FLIGHT_CLIENT_ENVIRONMENT=production
A2FLIGHT_CLIENT_VERSION=1.0.0
A2FLIGHT_CLIENT_SERVICE_NAME=a2db-flight-client

# ============================================================================
# OPTIONAL RESTRICTIONS
# ============================================================================
A2FLIGHT_CLIENT_ALLOWED_SERVER_IPS=
A2FLIGHT_CLIENT_ALLOWED_OPERATIONS=

# ============================================================================
# CONFIGURATION NOTES
# ============================================================================
# 1. REQUIRED: Set A2FLIGHT_CLIENT_API_KEY from server administrator
# 2. REQUIRED: Set A2FLIGHT_CLIENT_NAME to match server configuration
# 3. Update A2FLIGHT_SERVER_HOST and PORT for your deployment
# 4. Configure TLS settings if server requires encryption
# 5. Adjust connection pool size based on workload patterns
# 6. Enable compression if network bandwidth is a constraint
# 7. Review retry and timeout settings for your use case
# 8. Set LOG_API_KEYS to false in production environments
# 9. Max message size must be between 1MB-1GB (validated at startup)
# 10. Client config is separate from server's a2cl_config.env
# ============================================================================
"""

    try:
        with open(env_path, 'w') as f:
            f.write(content)
        log(f"Client configuration written to: {env_path.name}")

        # Set restrictive file permissions
        try:
            os.chmod(env_path, 0o600)
            log("File permissions set to 600 (owner read/write only)")
        except Exception:
            log("SECURITY WARNING: Set file permissions manually: chmod 600 a2flight_cl.env")

        return True
    except Exception as e:
        log(f"ERROR: Failed to write configuration file: {e}")
        return False


def generate_client_configuration():
    """Main function to generate Arrow Flight client configuration"""
    try:
        print("\n" + "=" * 80)
        print("A2DB Arrow Flight Client Configuration Generator")
        print("=" * 80 + "\n")

        specs = detect_system_specs()
        print()

        config = generate_client_config(specs)

        log("Configuration generated:")
        log(f"  Connection Pool Size: {config['CONNECTION_POOL_SIZE']}")
        log(f"  Max Connections: {config['MAX_CONNECTIONS']}")
        log(f"  Request Timeout: {config['REQUEST_TIMEOUT']} seconds")
        log(f"  Stream Chunk Size: {config['STREAM_CHUNK_SIZE']:,} rows")
        log(f"  Max Message Size: {config['MAX_MESSAGE_SIZE'] // (1024*1024)} MB")
        print()

        if not write_client_env_file(config, specs):
            return False
        print()

        print("=" * 80)
        print("Client Configuration Generation Complete")
        print("=" * 80 + "\n")

        print("CRITICAL NEXT STEPS:")
        print("  1. Contact server administrator to obtain:")
        print("     - Your API key")
        print("     - Confirm your client_name (default: 'etl')")
        print("  2. Update a2flight_cl.env:")
        print("     - Set A2FLIGHT_CLIENT_API_KEY=<your_key>")
        print("     - Set A2FLIGHT_SERVER_HOST=<server_address>")
        print("     - Set A2FLIGHT_SERVER_PORT=<server_port>")
        print("  3. Configure TLS if required by server")
        print("  4. Review and adjust connection pool settings")
        print("  5. Test connection before production use")
        print()

        print("SECURITY REMINDERS:")
        print("  - Never commit a2flight_cl.env with real API keys to version control")
        print("  - Set file permissions: chmod 600 a2flight_cl.env")
        print("  - Rotate API keys regularly (every 90 days recommended)")
        print("  - Keep API keys secure and never share them")
        print()

        return True

    except Exception as e:
        log(f"Configuration generation failed: {e}")
        return False


def main():
    """Main entry point"""
    import sys
    success = generate_client_configuration()
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()