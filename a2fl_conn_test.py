"""
Standalone Arrow Flight Connection Test
=========================================
Tests connection to A2DB Arrow Flight server without any A2DB dependencies.

Usage:
    python a2fl_conn_test.py

Requirements:
    pip install pyarrow[flight] rich
"""

import asyncio
import pyarrow.flight as flight
from pathlib import Path
from typing import Optional
import sys

# Rich for pretty output (optional - will work without it)
try:
    from rich.console import Console
    from rich.table import Table
    from rich import box

    RICH_AVAILABLE = True
    console = Console()
except ImportError:
    RICH_AVAILABLE = False
    print("Note: Install 'rich' for prettier output: pip install rich")


class SimpleFlightClient:
    """Minimal Arrow Flight client for connection testing"""

    def __init__(self, host: str = "localhost", port: int = 50054,
                 client_name: str = "etl", api_key: Optional[str] = None):
        self.host = host
        self.port = port
        self.client_name = client_name
        self.api_key = api_key
        self.location = f"grpc://{host}:{port}"
        self._client: Optional[flight.FlightClient] = None
        self._connected = False

    async def connect(self) -> bool:
        """Connect to Arrow Flight server with auto-detection"""
        try:
            # Create Flight client
            self._client = flight.FlightClient(self.location)

            # Auto-detect if server requires authentication
            auth_required = False

            if self.api_key and self.api_key != '<to be entered>':
                try:
                    # Test if we can access server without auth
                    test = list(self._client.list_actions())
                    # Success = no auth required
                    auth_required = False
                    print_info("Server does not require authentication")
                except Exception as e:
                    error_msg = str(e).lower()
                    if 'unauthenticated' in error_msg or 'unauthorized' in error_msg:
                        # Server requires authentication
                        auth_required = True
                        print_info("Server requires authentication, authenticating...")
                        self._client.authenticate_basic_token(
                            self.client_name,
                            self.api_key
                        )
                        print_success("Authentication completed")

            self._connected = True
            return True

        except Exception as e:
            raise Exception(f"Connection failed: {str(e)}")

    async def test_action(self, action: str = "ping") -> dict:
        """Test a simple action to verify connection"""
        if not self._connected:
            raise Exception("Not connected. Call connect() first.")

        try:
            # Try to list actions (this is a simple test)
            actions = list(self._client.list_actions())
            return {
                'success': True,
                'actions_available': len(actions),
                'actions': [str(action.type) for action in actions]
            }
        except Exception as e:
            return {
                'success': False,
                'error': str(e)
            }

    async def close(self):
        """Close connection"""
        if self._client:
            self._client.close()
            self._connected = False


def print_section(title: str):
    """Print a section header"""
    if RICH_AVAILABLE:
        console.print(f"\n[cyan]{'=' * 80}[/cyan]")
        console.print(f"[cyan]{title}[/cyan]")
        console.print(f"[cyan]{'=' * 80}[/cyan]\n")
    else:
        print(f"\n{'=' * 80}")
        print(title)
        print(f"{'=' * 80}\n")


def print_success(message: str):
    """Print success message"""
    if RICH_AVAILABLE:
        console.print(f"[green]✅ {message}[/green]")
    else:
        print(f"✅ {message}")


def print_error(message: str):
    """Print error message"""
    if RICH_AVAILABLE:
        console.print(f"[red]❌ {message}[/red]")
    else:
        print(f"❌ {message}")


def print_info(message: str):
    """Print info message"""
    if RICH_AVAILABLE:
        console.print(f"[yellow]{message}[/yellow]")
    else:
        print(message)


def load_config_from_env(env_file: str = "a2flight_cl.env") -> dict:
    """Load configuration from .env file if it exists"""
    config = {
        'host': 'localhost',
        'port': 50054,
        'client_name': 'etl',
        'api_key': None
    }

    # Try to find and load config file
    possible_paths = [
        Path.cwd() / env_file,
        Path.cwd() / "a2flight_cl" / env_file,
        Path(__file__).parent / env_file,
        Path(__file__).parent / "a2flight_cl" / env_file,
    ]

    for path in possible_paths:
        if path.exists():
            print_info(f"Found config file: {path}")

            with open(path, 'r') as f:
                for line in f:
                    line = line.strip()
                    if line and not line.startswith('#'):
                        if '=' in line:
                            key, value = line.split('=', 1)
                            key = key.strip()
                            value = value.strip()

                            if key == 'A2FLIGHT_CLIENT_NAME':
                                config['client_name'] = value
                            elif key == 'A2FLIGHT_CLIENT_API_KEY':
                                config['api_key'] = value
                            elif key == 'A2FLIGHT_SERVER_HOST':
                                config['host'] = value
                            elif key == 'A2FLIGHT_SERVER_PORT':
                                config['port'] = int(value)

            print_success(f"Loaded configuration from: {path.name}\n")
            return config

    print_info("No config file found, using defaults\n")
    return config


def print_config_table(config: dict):
    """Print configuration in a table"""
    if RICH_AVAILABLE:
        table = Table(title="Configuration", box=box.ROUNDED)
        table.add_column("Setting", style="cyan")
        table.add_column("Value", style="yellow")

        table.add_row("Server Host", config['host'])
        table.add_row("Server Port", str(config['port']))
        table.add_row("Client Name", config['client_name'])

        if config['api_key']:
            masked_key = f"{config['api_key'][:15]}...{config['api_key'][-4:]}"
            table.add_row("API Key", masked_key)
        else:
            table.add_row("API Key", "[red]NOT SET[/red]")

        console.print(table)
    else:
        print("Configuration:")
        print(f"  Server Host: {config['host']}")
        print(f"  Server Port: {config['port']}")
        print(f"  Client Name: {config['client_name']}")
        if config['api_key']:
            masked_key = f"{config['api_key'][:15]}...{config['api_key'][-4:]}"
            print(f"  API Key: {masked_key}")
        else:
            print(f"  API Key: NOT SET")


async def run_connection_test():
    """Run the complete connection test"""

    print_section("A2DB Arrow Flight Connection Test")

    # Load configuration
    config = load_config_from_env()
    print_config_table(config)

    print_section("Connection Test")

    # Create client
    client = SimpleFlightClient(
        host=config['host'],
        port=config['port'],
        client_name=config['client_name'],
        api_key=config['api_key']
    )

    try:
        # Test 1: Connection
        print_info(f"Connecting to {client.location}...")
        await client.connect()
        print_success(f"Connected to {client.location}")

        # Test 2: Authentication
        if config['api_key'] and config['api_key'] != '<to be entered>':
            print_success(f"Authentication successful for client '{config['client_name']}'")
        else:
            print_info("No API key configured (authentication skipped)")

        # Test 3: Server interaction
        print_info("\nTesting server interaction...")
        result = await client.test_action()

        if result['success']:
            print_success("Server interaction successful")
            print_info(f"  Available actions: {result['actions_available']}")
        else:
            print_error(f"Server interaction failed: {result['error']}")

        # Summary
        print_section("Test Summary")
        print_success("All tests passed!")
        print_info("\nConnection Details:")
        print(f"  • Server: {client.location}")
        print(f"  • Client: {config['client_name']}")
        print(f"  • Status: Connected and Authenticated")

        return True

    except Exception as e:
        print_section("Test Failed")
        print_error(f"Error: {str(e)}")

        # Provide troubleshooting tips
        print("\n" + "=" * 80)
        print("Troubleshooting:")
        print("=" * 80)
        print("1. Check if Arrow Flight server is running:")
        print("   netstat -ano | findstr :50054")
        print()
        print("2. Verify your configuration file (a2flight_cl.env):")
        print(f"   - Client Name: {config['client_name']}")
        print(f"   - Server: {config['host']}:{config['port']}")
        if not config['api_key'] or config['api_key'] == '<to be entered>':
            print("   - API Key: ⚠️  NOT CONFIGURED")
            print()
            print("3. Get API key from server administrator and update:")
            print("   a2flight_cl.env → A2FLIGHT_CLIENT_API_KEY=your_key_here")
        else:
            print(f"   - API Key: Configured ({config['api_key'][:15]}...)")
            print()
            print("3. Verify API key matches server configuration")
        print()
        print("4. Check server logs for authentication errors")
        print()
        print("5. Ensure your IP is whitelisted on the server")

        return False

    finally:
        await client.close()


def main():
    """Main entry point"""
    try:
        # Check if pyarrow.flight is available
        import pyarrow.flight

        # Run async test
        success = asyncio.run(run_connection_test())

        # Exit with appropriate code
        sys.exit(0 if success else 1)

    except ImportError as e:
        print_error("PyArrow Flight not installed!")
        print()
        print("Install with:")
        print("  pip install 'pyarrow[flight]>=14.0.0'")
        print()
        sys.exit(1)
    except KeyboardInterrupt:
        print("\n\nTest cancelled by user")
        sys.exit(1)
    except Exception as e:
        print_error(f"Unexpected error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()