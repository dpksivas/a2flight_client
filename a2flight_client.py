"""
A2DB Arrow Flight Client - API Method Wrapper v2.4 - AUTHENTICATION FIXED
=================================================================
Zero-serialization with GENERATOR-BASED memory efficiency + Clean API

VERSION 2.4 CRITICAL FIX:
âœ… AUTHENTICATION ENABLED - No longer commented out!
âœ… Conditional auth based on API key configuration
âœ… Enhanced error messages for auth failures
âœ… Server connectivity validation

VERSION 2.3 IMPROVEMENTS (RETAINED):
- Module-level API: a2db_ins, a2db_sel, a2db_updt, a2db_del
- Clean import pattern: import a2flight_client as a2fl
- Global client management with auto-reconnection
- Backward compatible with all v2.2 features

CRITICAL FIX SUMMARY:
âŒ BEFORE: self._authenticate() was commented out
âœ… AFTER: Authentication enabled with proper validation
âœ… Smart detection: Only authenticates if valid API key configured
âœ… Clear error messages for auth failures (IP blocked, invalid key, etc.)

Author: A2DB Team
Version: 2.4 - Authentication Fixed
Date: October 04, 2025
"""
import pyarrow.flight as flight
import pyarrow as pa
import time
import logging
import io
from pathlib import Path
from typing import Optional, Dict, Any, AsyncIterator, List
from contextlib import asynccontextmanager
from rich.console import Console
from rich.logging import RichHandler
from a2flight_cl_config import (
    get_flight_client_config,
    A2FlightClientConfig
)

# Setup logging with both file and console handlers
def setup_logger():
    """Setup logger with file and console output"""
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)

    # Clear any existing handlers
    logger.handlers.clear()

    # File handler - logs to a2cl_logger.log
    log_file = Path(__file__).parent / "a2cl_logger.log"
    file_handler = logging.FileHandler(log_file, mode='a', encoding='utf-8')
    file_handler.setLevel(logging.INFO)
    file_formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    file_handler.setFormatter(file_formatter)

    # Console handler with Rich
    console_handler = RichHandler(
        rich_tracebacks=True,
        markup=True,
        show_time=False,
        show_path=False
    )
    console_handler.setLevel(logging.INFO)

    # Add both handlers
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)

    return logger

logger = setup_logger()
console = Console()


class A2FlightClientError(Exception):
    """Base exception for Arrow Flight client errors"""
    pass


class A2FlightClient:
    """
    Ultra Production Arrow Flight Client - AUTHENTICATION FIXED

    CRITICAL FIX v2.4:
    âœ… Authentication is now ENABLED by default
    âœ… Smart API key detection (only auth if valid key configured)
    âœ… Enhanced error messages for common auth failures
    âœ… Server connectivity validation

    Principles:
    - Fail-fast error handling
    - Zero client-side validation
    - Minimal API surface
    - Direct server delegation
    - SECURE by default

    Usage:
        async with A2FlightClient() as client:
            # INSERT
            result = await client.insert(arrow_table, 'users')

            # SELECT
            result_table = await client.select('SELECT * FROM users')

            # UPDATE
            result = await client.update(arrow_table, 'users')

            # DELETE
            result = await client.delete(arrow_table, 'users')
    """

    def __init__(self, config: Optional[A2FlightClientConfig] = None):
        """
        Initialize Arrow Flight client

        Args:
            config: Optional config instance (loads from env if None)
        """
        self.config = config or get_flight_client_config()
        self._client: Optional[flight.FlightClient] = None
        self._connected = False

        logger.info(
            f"Arrow Flight client initialized: '{self.config.client_name}'"
        )

    async def connect(self) -> bool:
        """
        Connect to Arrow Flight server with auto-detection of authentication

        Returns:
            bool: True if connection successful
        """
        try:
            logger.info(f"Connecting to {self.config.server_location}...")

            # Create Flight client
            self._client = flight.FlightClient(self.config.server_location)

            # Auto-detect if server requires authentication
            auth_required = False

            if self.config.api_key and self.config.api_key != '<to be entered>':
                try:
                    # Test server without auth first
                    list(self._client.list_actions())
                    # If we got here, server doesn't require auth
                    logger.info("Server does not require authentication")
                    auth_required = False
                except Exception as e:
                    # Check if it's an auth error
                    if 'unauthenticated' in str(e).lower() or 'unauthorized' in str(e).lower():
                        # Server requires auth
                        logger.info(f"Authenticating as '{self.config.client_name}'...")
                        self._authenticate()
                        logger.info("Authentication successful!")
                        auth_required = True
                    elif 'unimplemented' in str(e).lower() and 'authentication' in str(e).lower():
                        # Server has auth disabled but we tried to auth
                        logger.info("Server does not require authentication")
                        auth_required = False
                    else:
                        # Some other error - re-raise
                        raise

            self._connected = True
            logger.info(f"Connected to '{self.config.server_location}'")

            return True

        except Exception as e:
            error_str = str(e).lower()
            if 'unavailable' in error_str or 'failed to connect' in error_str or 'connection refused' in error_str:
                simple_msg = f"Arrow Flight server is not running at {self.config.server_location}"
                logger.error(simple_msg)
                raise A2FlightClientError(simple_msg)
            else:
                logger.error(f"Connection failed: {e}")
                raise A2FlightClientError(f"Connection failed: {e}")

    def _authenticate(self):
        """
        Authenticate with server using API key - ENHANCED ERROR MESSAGES

        âœ… CRITICAL: This method is now CALLED (not commented out!)
        âœ… Enhanced error messages for common auth failures
        âœ… Specific guidance for IP whitelisting issues

        Raises:
            A2FlightClientError: Authentication failure with specific reason
        """
        try:
            self._client.authenticate_basic_token(
                username=self.config.client_name,
                password=self.config.api_key
            )
            logger.debug(
                f"Authentication successful for client '{self.config.client_name}'"
            )

        except Exception as e:
            error_str = str(e).lower()

            # Provide specific error messages for common failures
            if 'invalid api key' in error_str:
                raise A2FlightClientError(
                    f"Authentication failed: Invalid API key for client '{self.config.client_name}'.\n"
                    f"Verify API key with server administrator.\n"
                    f"Current API key starts with: {self.config.api_key[:12]}..."
                )
            elif 'ip not whitelisted' in error_str or 'access denied' in error_str:
                raise A2FlightClientError(
                    f"Authentication failed: IP address not whitelisted for client '{self.config.client_name}'.\n"
                    f"Contact server administrator to whitelist your IP address.\n"
                    f"Check server logs for your detected IP address."
                )
            elif 'client disabled' in error_str:
                raise A2FlightClientError(
                    f"Authentication failed: Client '{self.config.client_name}' is disabled.\n"
                    f"Contact server administrator to enable your client."
                )
            elif 'unauthenticated' in error_str:
                raise A2FlightClientError(
                    f"Authentication failed for client '{self.config.client_name}'.\n"
                    f"Possible reasons:\n"
                    f"  - Invalid API key\n"
                    f"  - IP address not whitelisted\n"
                    f"  - Client disabled on server\n"
                    f"Original error: {str(e)}"
                )
            else:
                logger.error(f"Authentication failed: {e}")
                raise A2FlightClientError(f"Authentication failed: {e}")

    def _ensure_connected(self):
        """Ensure client is connected"""
        if not self._connected or self._client is None:
            raise A2FlightClientError(
                "Not connected. Call connect() first."
            )

    # ========================================================================
    # INSERT Operation
    # ========================================================================
    async def insert(
            self,
            arrow_table: pa.Table,
            table_name: str
    ) -> Dict[str, Any]:
        """INSERT data via Arrow Flight"""
        self._ensure_connected()
        start_time = time.time()

        try:
            logger.info(f"INSERT {arrow_table.num_rows:,} rows -> '{table_name}'")

            # Build descriptor
            descriptor = flight.FlightDescriptor.for_path(table_name.encode('utf-8'))

            # Create writer
            writer, metadata_reader = self._client.do_put(
                descriptor,
                arrow_table.schema
            )

            # Write data
            writer.write_table(arrow_table)
            writer.close()

            execution_time = (time.time() - start_time) * 1000

            result = {
                'success': True,
                'rows_written': arrow_table.num_rows,
                'method_used': 'flight',
                'execution_time_ms': execution_time
            }

            logger.info(
                f"INSERT complete: {result['rows_written']:,} rows "
                f"in {result['execution_time_ms']:.1f}ms"
            )

            return result

        except Exception as e:
            error_str = str(e).lower()
            if 'unavailable' in error_str or 'failed to connect' in error_str:
                simple_msg = f"Arrow Flight server is not running at {self.config.server_location}"
                logger.error(simple_msg)
                raise A2FlightClientError(simple_msg)
            else:
                logger.error(f"INSERT failed: {e}", exc_info=True)
                raise A2FlightClientError(f"INSERT failed: {e}")

    # ========================================================================
    # SELECT Operation
    # ========================================================================
    async def select(
            self,
            query: str,
            params: Optional[List] = None
    ) -> pa.Table:
        """
        SELECT data via Arrow Flight

        Args:
            query: SQL query string
            params: Optional query parameters (reserved for future use)

        Returns:
            PyArrow table with results

        Raises:
            A2FlightClientError: Operation failure
        """
        self._ensure_connected()
        start_time = time.time()

        try:
            logger.info(f"SELECT: {query[:100]}...")

            # Create ticket with query
            ticket = flight.Ticket(query.encode('utf-8'))

            # Get reader and read results
            reader = self._client.do_get(ticket)
            result_table = reader.read_all()

            execution_time = (time.time() - start_time) * 1000

            logger.info(
                f"SELECT complete: {result_table.num_rows:,} rows "
                f"in {execution_time:.1f}ms"
            )

            return result_table

        except Exception as e:
            logger.error(f"SELECT failed: {e}", exc_info=True)
            raise A2FlightClientError(f"SELECT failed: {e}")

    async def select_streaming(
            self,
            query: str
    ) -> AsyncIterator[pa.Table]:
        """
        SELECT with streaming for large datasets

        Args:
            query: SQL query string

        Yields:
            PyArrow table chunks

        Raises:
            A2FlightClientError: Operation failure
        """
        self._ensure_connected()

        try:
            logger.info(f"SELECT (streaming): {query[:100]}...")

            ticket = flight.Ticket(query.encode('utf-8'))
            reader = self._client.do_get(ticket)

            for chunk in reader:
                yield chunk.data

            logger.info("SELECT streaming complete")

        except Exception as e:
            logger.error(f"SELECT streaming failed: {e}", exc_info=True)
            raise A2FlightClientError(f"SELECT streaming failed: {e}")

    # ========================================================================
    # UPDATE Operation
    # ========================================================================
    async def update(
            self,
            arrow_table: pa.Table,
            table_name: str
    ) -> Dict[str, Any]:
        """
        UPDATE data via Arrow Flight

        Args:
            arrow_table: PyArrow table with update data
            table_name: Target table name

        Returns:
            Dict with results: rows_affected, execution_time_ms, method_used

        Raises:
            A2FlightClientError: Operation failure
        """
        self._ensure_connected()
        start_time = time.time()

        try:
            logger.info(
                f"UPDATE {arrow_table.num_rows:,} rows -> '{table_name}'"
            )

            # Serialize data table to Arrow IPC
            data_sink = io.BytesIO()
            data_writer = pa.ipc.new_stream(data_sink, arrow_table.schema)
            data_writer.write_table(arrow_table)
            data_writer.close()
            data_bytes = data_sink.getvalue()

            # Create metadata table
            metadata_table = pa.table({
                'table_name': [table_name],
                'data': [data_bytes]
            })

            # Serialize metadata table
            meta_sink = io.BytesIO()
            meta_writer = pa.ipc.new_stream(meta_sink, metadata_table.schema)
            meta_writer.write_table(metadata_table)
            meta_writer.close()

            # Create action
            action = flight.Action('update', meta_sink.getvalue())

            # Execute action
            result_stream = self._client.do_action(action)

            # Read Arrow IPC response
            result_bytes = b''
            for result in result_stream:
                result_bytes += result.body

            # Deserialize response
            response_buffer = pa.py_buffer(result_bytes)
            response_reader = pa.ipc.open_stream(response_buffer)
            response_table = response_reader.read_all()

            # Extract result
            result = {
                'success': response_table['success'][0].as_py(),
                'rows_affected': response_table['rows_affected'][0].as_py(),
                'execution_time_ms': (time.time() - start_time) * 1000,
                'method_used': (
                    response_table['method_used'][0].as_py()
                    if 'method_used' in response_table.schema.names
                    else None
                ),
            }

            logger.info(
                f"UPDATE complete: {result['rows_affected']:,} rows "
                f"in {result['execution_time_ms']:.1f}ms"
            )

            return result

        except Exception as e:
            logger.error(f"UPDATE failed: {e}", exc_info=True)
            raise A2FlightClientError(f"UPDATE failed: {e}")

    # ========================================================================
    # DELETE Operation
    # ========================================================================
    async def delete(
            self,
            arrow_table: pa.Table,
            table_name: str
    ) -> Dict[str, Any]:
        """
        DELETE data via Arrow Flight

        Args:
            arrow_table: PyArrow table with rows to delete (typically IDs)
            table_name: Target table name

        Returns:
            Dict with results: rows_affected, execution_time_ms, method_used

        Raises:
            A2FlightClientError: Operation failure
        """
        self._ensure_connected()
        start_time = time.time()

        try:
            logger.info(
                f"DELETE {arrow_table.num_rows:,} rows <- '{table_name}'"
            )

            # Serialize data table to Arrow IPC
            data_sink = io.BytesIO()
            data_writer = pa.ipc.new_stream(data_sink, arrow_table.schema)
            data_writer.write_table(arrow_table)
            data_writer.close()
            data_bytes = data_sink.getvalue()

            # Create metadata table
            metadata_table = pa.table({
                'table_name': [table_name],
                'data': [data_bytes]
            })

            # Serialize metadata table
            meta_sink = io.BytesIO()
            meta_writer = pa.ipc.new_stream(meta_sink, metadata_table.schema)
            meta_writer.write_table(metadata_table)
            meta_writer.close()

            # Create action
            action = flight.Action('delete', meta_sink.getvalue())

            # Execute action
            result_stream = self._client.do_action(action)

            # Read Arrow IPC response
            result_bytes = b''
            for result in result_stream:
                result_bytes += result.body

            # Deserialize response
            response_buffer = pa.py_buffer(result_bytes)
            response_reader = pa.ipc.open_stream(response_buffer)
            response_table = response_reader.read_all()

            # Extract result
            result = {
                'success': response_table['success'][0].as_py(),
                'rows_affected': response_table['rows_affected'][0].as_py(),
                'execution_time_ms': (time.time() - start_time) * 1000,
                'method_used': (
                    response_table['method_used'][0].as_py()
                    if 'method_used' in response_table.schema.names
                    else None
                ),
            }

            logger.info(
                f"DELETE complete: {result['rows_affected']:,} rows "
                f"in {result['execution_time_ms']:.1f}ms"
            )

            return result

        except Exception as e:
            logger.error(f"DELETE failed: {e}", exc_info=True)
            raise A2FlightClientError(f"DELETE failed: {e}")

    # ========================================================================
    # Connection Management
    # ========================================================================
    async def close(self):
        """Close connection to server"""
        if self._client:
            try:
                self._client.close()
                logger.info("Connection closed")
            except Exception as e:
                logger.warning(f"Error closing connection: {e}")
            finally:
                self._client = None
                self._connected = False

    @property
    def is_connected(self) -> bool:
        """Check if client is connected"""
        return self._connected and self._client is not None

    def get_stats(self) -> Dict[str, Any]:
        """Get client statistics"""
        return {
            'connected': self.is_connected,
            'client_name': self.config.client_name,
            'server_location': self.config.server_location
        }

    def __enter__(self):
        """Sync context manager not supported"""
        raise NotImplementedError(
            "Use 'async with' for async context manager"
        )

    async def __aenter__(self):
        """Async context manager entry"""
        await self.connect()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit"""
        await self.close()
        return False


# ============================================================================
# Module-Level API Functions (v2.3 - Retained)
# ============================================================================

# Global client instance (lazy initialization)
_global_client: Optional[A2FlightClient] = None


async def _get_global_client() -> A2FlightClient:
    """Get or create global client instance"""
    global _global_client

    if _global_client is None or not _global_client.is_connected:
        _global_client = A2FlightClient()
        await _global_client.connect()

    return _global_client


async def a2db_ins(
    arrow_table: pa.Table,
    table_name: str
) -> Dict[str, Any]:
    """
    INSERT operation via Arrow Flight

    Usage:
        import a2flight_client as a2fl
        result = await a2fl.a2db_ins(arrow_table, 'users')

    Args:
        arrow_table: PyArrow table with data to insert
        table_name: Target PostgreSQL table name

    Returns:
        Dict with success, rows_written, execution_time_ms
    """
    client = await _get_global_client()
    return await client.insert(arrow_table, table_name)


async def a2db_sel(
    query: str,
    params: Optional[List] = None
) -> pa.Table:
    """
    SELECT operation via Arrow Flight

    Usage:
        import a2flight_client as a2fl
        result = await a2fl.a2db_sel('SELECT * FROM users WHERE id > 100')

    Args:
        query: SQL SELECT query string
        params: Optional query parameters (reserved for future use)

    Returns:
        PyArrow table with query results
    """
    client = await _get_global_client()
    return await client.select(query, params)


async def a2db_updt(
    arrow_table: pa.Table,
    table_name: str
) -> Dict[str, Any]:
    """
    UPDATE operation via Arrow Flight

    Usage:
        import a2flight_client as a2fl
        result = await a2fl.a2db_updt(arrow_table, 'users')

    Args:
        arrow_table: PyArrow table with update data (must include primary keys)
        table_name: Target PostgreSQL table name

    Returns:
        Dict with success, rows_affected, execution_time_ms
    """
    client = await _get_global_client()
    return await client.update(arrow_table, table_name)


async def a2db_del(
    arrow_table: pa.Table,
    table_name: str
) -> Dict[str, Any]:
    """
    DELETE operation via Arrow Flight

    Usage:
        import a2flight_client as a2fl
        result = await a2fl.a2db_del(arrow_table, 'users')

    Args:
        arrow_table: PyArrow table with rows to delete (primary keys)
        table_name: Target PostgreSQL table name

    Returns:
        Dict with success, rows_affected, execution_time_ms
    """
    client = await _get_global_client()
    return await client.delete(arrow_table, table_name)


async def a2db_close():
    """
    Close global client connection

    Usage:
        import a2flight_client as a2fl
        await a2fl.a2db_close()
    """
    global _global_client

    if _global_client is not None:
        await _global_client.close()
        _global_client = None


# ============================================================================
# Helper Functions
# ============================================================================

@asynccontextmanager
async def create_flight_client(
        config: Optional[A2FlightClientConfig] = None
) -> AsyncIterator[A2FlightClient]:
    """
    Create and connect Arrow Flight client

    Usage:
        async with create_flight_client() as client:
            result = await client.insert(table, 'users')

    Args:
        config: Optional config instance

    Yields:
        Connected A2FlightClient instance
    """
    client = A2FlightClient(config)
    try:
        await client.connect()
        yield client
    finally:
        await client.close()