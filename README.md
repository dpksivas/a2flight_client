# A2DB Arrow Flight Client

**High-Performance Python Client for A2DB Arrow Flight Server**

[![Python 3.10+](https://img.shields.io/badge/python-3.10+-blue.svg)](https://www.python.org/downloads/)
[![PyArrow 21.0+](https://img.shields.io/badge/pyarrow-21.0+-green.svg)](https://arrow.apache.org/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

Zero-serialization, PyArrow-first client library for connecting to A2DB Arrow Flight servers with enterprise-grade authentication and performance.

---

## Quick Start

### Installation

```bash
# Install dependencies
pip install pyarrow rich

# Clone and navigate
git clone https://github.com/your-org/a2db-flight-client.git
cd a2db-flight-client
```

### Basic Usage

```python
import a2flight_client as a2fl
import pyarrow as pa
from datetime import datetime

# Create test data
data = pa.table({
    'id': [1, 2, 3],
    'name': ['Alice', 'Bob', 'Charlie'],
    'created_at': [datetime.now()] * 3
})

# INSERT
result = await a2fl.a2db_ins(data, 'users')
print(f"Inserted {result['rows_written']} rows")

# SELECT
query = "SELECT * FROM users WHERE id > 100"
results = await a2fl.a2db_sel(query)
print(f"Retrieved {results.num_rows} rows")

# UPDATE
updated_data = results.set_column(
    results.schema.get_field_index('name'),
    'name',
    pa.array(['Updated'] * results.num_rows)
)
result = await a2fl.a2db_updt(updated_data, 'users')

# DELETE
result = await a2fl.a2db_del(results, 'users')

# Cleanup
await a2fl.a2db_close()
```

---

## Features

### Core Capabilities
- **Zero-Serialization Transport**: Native PyArrow IPC for maximum performance
- **Module-Level API**: Clean, Pythonic interface (`a2db_ins`, `a2db_sel`, `a2db_updt`, `a2db_del`)
- **Auto-Connection Management**: Global client with automatic reconnection
- **Streaming Support**: Memory-efficient processing of large datasets
- **Type Safety**: Full PyArrow Table I/O with schema preservation

### Security
- **Auto-Detection Authentication**: Seamlessly handles auth-enabled/disabled servers
- **API Key Management**: Secure credential handling via environment variables
- **IP Whitelisting**: Server-side validation (transparent to client)
- **TLS Support**: Optional encrypted transport

### Performance
- **SELECT**: 600K-700K RPS (records per second)
- **INSERT**: 80K-130K RPS
- **UPDATE**: 130K-300K RPS
- **DELETE**: 150K-200K RPS
- **Connection Pooling**: Optimized concurrent request handling

---

## Configuration

### Environment File Setup

Create `a2flight_cl.env` in your project root:

```ini
# Client Identity
A2FLIGHT_CLIENT_NAME=etl
A2FLIGHT_CLIENT_API_KEY=a2db_key_a2_54375_175930943680_xkysE

# Server Connection
A2FLIGHT_SERVER_HOST=localhost
A2FLIGHT_SERVER_PORT=50054
A2FLIGHT_CONNECTION_PROTOCOL=grpc

# Connection Pool
A2FLIGHT_CLIENT_CONNECTION_POOL_SIZE=8
A2FLIGHT_CLIENT_MAX_CONNECTIONS=16

# Request Settings
A2FLIGHT_CLIENT_REQUEST_TIMEOUT=300
A2FLIGHT_CLIENT_MAX_RETRIES=3
A2FLIGHT_CLIENT_STREAM_CHUNK_SIZE=500000
```

### Configuration Generator

```python
from a2flight_cl_config import generate_client_config

# Generate default configuration file
generate_client_config()
# Creates a2flight_cl.env with system-optimized defaults
```

---

## API Reference

### Module-Level Functions

#### `a2db_ins(arrow_table, table_name)`
Insert data into a table.

**Parameters:**
- `arrow_table` (pa.Table): PyArrow table with data to insert
- `table_name` (str): Target PostgreSQL table name

**Returns:**
- `dict`: `{'success': bool, 'rows_written': int, 'execution_time_ms': float, 'method_used': str}`

**Example:**
```python
import pyarrow as pa
import a2flight_client as a2fl

data = pa.table({'id': [1, 2], 'name': ['Alice', 'Bob']})
result = await a2fl.a2db_ins(data, 'users')
```

---

#### `a2db_sel(query, params=None)`
Execute a SELECT query.

**Parameters:**
- `query` (str): SQL SELECT query string
- `params` (List, optional): Query parameters (reserved for future use)

**Returns:**
- `pa.Table`: PyArrow table with query results

**Example:**
```python
results = await a2fl.a2db_sel('SELECT * FROM users WHERE id > 100')
print(f"Retrieved {results.num_rows} rows")
```

---

#### `a2db_updt(arrow_table, table_name)`
Update existing records.

**Parameters:**
- `arrow_table` (pa.Table): PyArrow table with updated data (must include primary keys)
- `table_name` (str): Target PostgreSQL table name

**Returns:**
- `dict`: `{'success': bool, 'rows_affected': int, 'execution_time_ms': float}`

**Example:**
```python
# Fetch data
data = await a2fl.a2db_sel('SELECT * FROM users LIMIT 10')

# Modify column
updated = data.set_column(
    data.schema.get_field_index('status'),
    'status',
    pa.array(['active'] * data.num_rows)
)

# Update
result = await a2fl.a2db_updt(updated, 'users')
```

---

#### `a2db_del(arrow_table, table_name)`
Delete records from a table.

**Parameters:**
- `arrow_table` (pa.Table): PyArrow table with rows to delete (primary keys)
- `table_name` (str): Target PostgreSQL table name

**Returns:**
- `dict`: `{'success': bool, 'rows_affected': int, 'execution_time_ms': float}`

**Example:**
```python
# Select rows to delete
to_delete = await a2fl.a2db_sel('SELECT id, created_at FROM users WHERE status = "inactive"')

# Delete
result = await a2fl.a2db_del(to_delete, 'users')
```

---

#### `a2db_close()`
Close the global client connection.

**Example:**
```python
await a2fl.a2db_close()
```

---

### Class-Based API (Advanced)

For fine-grained control, use the `A2FlightClient` class:

```python
from a2flight_client import A2FlightClient

async with A2FlightClient() as client:
    # INSERT
    result = await client.insert(arrow_table, 'users')
    
    # SELECT
    results = await client.select('SELECT * FROM users')
    
    # UPDATE
    result = await client.update(arrow_table, 'users')
    
    # DELETE
    result = await client.delete(arrow_table, 'users')
```

---

## Testing & Validation

### Connection Test

```bash
python a2fl_conn_test.py
```

Expected output:
```
✓ Connected to grpc://localhost:50054
✓ Authentication: DISABLED
✓ Server interaction successful
✓ Available actions: 2
```

### CRUD Performance Test

```bash
python a2fl_dl_tst_ops.py
```

Runs comprehensive performance tests across batch sizes from 1K to 1.5M rows with detailed metrics.

---

## Performance Benchmarks

Tested on: Intel i7-12700K, 32GB RAM, PostgreSQL 17, localhost connection

| Operation | Batch Size | RPS | Grade | Time (ms) |
|-----------|------------|-----|-------|-----------|
| **SELECT** | 1M rows | 700,693 | A+ | 1,427.2 |
| **INSERT** | 150K rows | 130,248 | A- | 1,151.6 |
| **UPDATE** | 750K rows | 302,193 | A+ | 2,481.8 |
| **DELETE** | 300K rows | 181,985 | A | 1,648.5 |

**Performance Grading Scale:**
- A++: ≥1M RPS
- A+: ≥500K RPS  
- A: ≥250K RPS
- A-: ≥100K RPS (Financial services baseline)
- B+: ≥50K RPS

---

## Security Best Practices

### API Key Management

1. **Never commit API keys** to version control
2. Use environment variables or `.env` files (add to `.gitignore`)
3. Rotate keys periodically
4. Use different keys per environment (dev/staging/prod)

```bash
# .gitignore
a2flight_cl.env
*.env
```

### Obtaining API Keys

Contact your A2DB server administrator to obtain:
- Client name (e.g., `etl`)
- API key (e.g., `a2db_key_a2_54375...`)
- Server host and port
- Whitelisted IP addresses

---

## Troubleshooting

### Authentication Errors

**Error:** `Flight returned unimplemented error: This service does not have an authentication mechanism enabled`

**Solution:** The server has authentication disabled. The client auto-detects this and connects without authenticating.

---

**Error:** `Authentication failed: Invalid API key`

**Solution:** 
1. Verify API key in `a2flight_cl.env` matches server configuration
2. Check for typos or extra whitespace
3. Confirm key hasn't been revoked

---

### Connection Issues

**Error:** `Arrow Flight server is not running`

**Solution:**
1. Verify server is running: `netstat -ano | findstr :50054`
2. Check firewall rules
3. Confirm host/port in config

---

### Performance Issues

**Symptom:** Lower than expected throughput

**Solutions:**
1. Increase connection pool size: `A2FLIGHT_CLIENT_CONNECTION_POOL_SIZE=16`
2. Adjust chunk size for large datasets: `A2FLIGHT_CLIENT_STREAM_CHUNK_SIZE=1000000`
3. Enable connection pooling on PostgreSQL
4. Check network latency between client and server

---

## Additional Resources

- [Apache Arrow Flight Documentation](https://arrow.apache.org/docs/format/Flight.html)
- [PyArrow API Reference](https://arrow.apache.org/docs/python/)
- [A2DB Server Repository](https://github.com/your-org/a2db-server)

---

## License

MIT License - see [LICENSE](LICENSE) file for details

---

## Contributing

Contributions welcome! Please:
1. Fork the repository
2. Create a feature branch
3. Submit a pull request with tests

---

## Support

- **Issues**: [GitHub Issues](https://github.com/your-org/a2db-flight-client/issues)
- **Discussions**: [GitHub Discussions](https://github.com/your-org/a2db-flight-client/discussions)
- **Email**: support@your-org.com

---

**Built with Apache Arrow and Python**
