"""
A2DB Arrow Flight Client - Main Test/Demo Script (CORRECTED)
==============================================================
Demonstrates all CRUD operations with the Arrow Flight client for a2users table
"""

import asyncio
import sys
from pathlib import Path

# Add a2flight_cl to path if needed
sys.path.insert(0, str(Path(__file__).parent / 'a2flight_cl'))

import a2flight_client as a2fl
import pyarrow as pa
from datetime import datetime, timezone


async def test_insert():
    """Test INSERT operation"""
    print("\n" + "=" * 80)
    print("Testing INSERT Operation")
    print("=" * 80)

    # Create test data matching a2users schema
    now = datetime.now(timezone.utc)

    data = pa.table({
        'a2user_id': [1001, 1002, 1003],
        'first_name': ['Alice', 'Bob', 'Charlie'],
        'last_name': ['Smith', 'Johnson', 'Williams'],
        'known_as': ['Ali', 'Bobby', None],
        'email': ['alice.smith@gmail.com', 'bob.johnson@gmail.com', 'charlie.williams@gmail.com'],
        'phone': ['+1234567890', '+1234567891', None],
        'eff_from': [now, now, now],
        'eff_to': [None, None, None],
        'is_active': [True, True, True],
        'remarks': ['Test user 1', 'Test user 2', 'Test user 3'],
        'created_at': [now, now, now]
    })

    print(f"Inserting {data.num_rows} rows...")
    result = await a2fl.a2db_ins(data, 'a2users')

    print(f"Result: {result}")
    print(f"  Rows written: {result['rows_written']}")
    print(f"  Execution time: {result['execution_time_ms']:.2f}ms")


async def test_select():
    """Test SELECT operation"""
    print("\n" + "=" * 80)
    print("Testing SELECT Operation")
    print("=" * 80)

    query = "SELECT * FROM a2users WHERE is_active = true LIMIT 10"
    print(f"Query: {query}")

    result = await a2fl.a2db_sel(query)

    print(f"\nResults:")
    print(f"  Rows returned: {result.num_rows}")
    print(f"  Columns: {result.column_names}")

    if result.num_rows > 0:
        print(f"\nFirst few rows:")
        print(result.to_pandas().head())
    else:
        print("\nNo rows returned")

    return result


async def test_update():
    """Test UPDATE operation"""
    print("\n" + "=" * 80)
    print("Testing UPDATE Operation")
    print("=" * 80)

    # First, fetch the existing record to get the actual eff_from value
    query = "SELECT * FROM a2users WHERE a2user_id = 1001 LIMIT 1"
    print(f"Fetching existing record: {query}")

    existing = await a2fl.a2db_sel(query)

    if existing.num_rows == 0:
        print("No record found with a2user_id = 1001, skipping update")
        return

    # Extract the actual eff_from value from the first row
    actual_eff_from = existing['eff_from'][0].as_py()
    print(f"Found record with eff_from: {actual_eff_from}")

    # Update data - use the actual eff_from from database
    update_data = pa.table({
        'a2user_id': [1001],
        'eff_from': [actual_eff_from],  # Use actual value from database
        'first_name': ['Alice'],
        'last_name': ['Smith-Updated'],
        'known_as': ['Alice'],
        'email': ['alice.smith.updated@gmail.com'],
        'phone': ['+1234567890'],
        'eff_to': [None],
        'is_active': [True],
        'remarks': ['Updated test user'],
        'created_at': [actual_eff_from]  # Keep original created_at
    })

    print(f"Updating {update_data.num_rows} rows...")
    result = await a2fl.a2db_updt(update_data, 'a2users')

    print(f"Result: {result}")
    print(f"  Rows affected: {result['rows_affected']}")
    print(f"  Execution time: {result['execution_time_ms']:.2f}ms")


async def test_delete():
    """Test DELETE operation"""
    print("\n" + "=" * 80)
    print("Testing DELETE Operation")
    print("=" * 80)

    # First, fetch the existing record to get the actual eff_from value
    query = "SELECT * FROM a2users WHERE a2user_id = 1003 LIMIT 1"
    print(f"Fetching existing record: {query}")

    existing = await a2fl.a2db_sel(query)

    if existing.num_rows == 0:
        print("No record found with a2user_id = 1003, skipping delete")
        return

    # Extract the actual eff_from value
    actual_eff_from = existing['eff_from'][0].as_py()
    print(f"Found record with eff_from: {actual_eff_from}")

    # Delete data - use the actual eff_from from database
    delete_data = pa.table({
        'a2user_id': [1003],
        'eff_from': [actual_eff_from]  # Use actual value from database
    })

    print(f"Deleting {delete_data.num_rows} rows...")
    result = await a2fl.a2db_del(delete_data, 'a2users')

    print(f"Result: {result}")
    print(f"  Rows affected: {result['rows_affected']}")
    print(f"  Execution time: {result['execution_time_ms']:.2f}ms")


async def main():
    """Main test function"""
    print("\n" + "=" * 80)
    print("A2DB Arrow Flight Client - CRUD Operations Test")
    print("a2users Table")
    print("=" * 80)

    try:
        # Test all operations
        await test_insert()
        await test_select()
        await test_update()
        await test_delete()

        print("\n" + "=" * 80)
        print("All tests completed successfully!")
        print("=" * 80)
        print("\nCheck 'a2cl_logger.log' for detailed logs")

    except a2fl.A2FlightClientError as e:
        print(f"\n❌ Arrow Flight Client Error: {e}")
        print("\n🔍 Troubleshooting:")
        print("  1. Ensure Arrow Flight server is running:")
        print("     cd a2flight_srvr && python -m a2flight_srvr.a2flight_server")
        print("  2. Verify a2flight_cl.env has correct configuration")
        print("  3. Check API key matches server configuration")
        print("  4. Verify IP is whitelisted on server")
        print("  5. Check firewall allows port 50054")

    except Exception as e:
        print(f"\n❌ Unexpected Error: {e}")
        print("\n🔍 Additional Troubleshooting:")
        print("  1. Email addresses must end with @gmail.com (constraint)")
        print("  2. Check a2user_id and eff_from are unique together (composite PK)")
        print("  3. Verify eff_to >= eff_from (constraint)")
        print("  4. Email and phone must be unique")
        print("  5. Check a2cl_logger.log for detailed error information")

    finally:
        await a2fl.a2db_close()
        print("\nConnection closed.")


if __name__ == "__main__":
    asyncio.run(main())