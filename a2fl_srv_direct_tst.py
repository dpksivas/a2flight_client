import asyncio
import pyarrow.flight as flight

async def test_direct():
    try:
        # Direct connection without any wrappers
        client = flight.FlightClient('grpc://localhost:50054')

        # Authenticate with exact credentials
        client.authenticate_basic_token('etl', 'a2db_key_a2_54375_175930943680_xkysE')

        print('✅ Authentication successful!')

        # Try listing actions as a test
        actions = list(client.list_actions())
        print(f'✅ Server responded: {len(actions)} actions available')

        client.close()
        return True

    except Exception as e:
        print(f'❌ Error: {e}')
        return False


asyncio.run(test_direct())