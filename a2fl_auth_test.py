"""
Arrow Flight Connection Test with Auto-Detection
=================================================
Automatically detects if server requires authentication
"""

import pyarrow.flight as flight
import sys

print('\n' + '=' * 80)
print('Arrow Flight Connection Test - Auto Authentication Detection')
print('=' * 80 + '\n')

# Configuration
SERVER_LOCATION = 'grpc://localhost:50054'
CLIENT_NAME = 'etl'
API_KEY = 'a2db_key_a2_54375_175930943680_xkysE'

try:
    # Step 1: Create client
    print('Step 1: Creating Flight client...')
    client = flight.FlightClient(SERVER_LOCATION)
    print(f'        Client created for {SERVER_LOCATION}')

    # Step 2: Test if authentication is required
    print('\nStep 2: Detecting authentication requirement...')

    auth_required = False
    try:
        # Try to list actions without authentication
        test_actions = list(client.list_actions())
        print('        Server does NOT require authentication')
        auth_required = False
    except flight.FlightUnauthenticatedError:
        print('        Server REQUIRES authentication')
        auth_required = True
    except Exception as e:
        if 'unauthenticated' in str(e).lower() or 'unauthorized' in str(e).lower():
            print('        Server REQUIRES authentication')
            auth_required = True
        else:
            # Some other error, might not be auth-related
            print(f'        Warning: Unexpected error during detection: {e}')
            auth_required = False

    # Step 3: Authenticate if required
    if auth_required:
        print('\nStep 3: Authenticating...')
        print(f'        Client name: {CLIENT_NAME}')
        print(f'        API key: {API_KEY[:15]}...')

        try:
            client.authenticate_basic_token(CLIENT_NAME, API_KEY)
            print('        Authentication successful')
        except Exception as auth_error:
            print(f'        Authentication FAILED: {auth_error}')
            raise
    else:
        print('\nStep 3: Skipping authentication (not required)')

    # Step 4: Test server communication
    print('\nStep 4: Testing server communication...')
    actions = list(client.list_actions())
    print(f'        Server responded with {len(actions)} actions available')

    # Success summary
    print('\n' + '=' * 80)
    print('CONNECTION TEST SUCCESSFUL')
    print('=' * 80)
    print(f'\nServer: {SERVER_LOCATION}')
    print(f'Authentication: {"ENABLED" if auth_required else "DISABLED"}')
    print(f'Status: Connected and operational')
    print('\n')

    client.close()
    sys.exit(0)

except flight.FlightUnauthenticatedError as e:
    print('\n' + '=' * 80)
    print('AUTHENTICATION FAILED')
    print('=' * 80)
    print(f'\nError: {e}')
    print('\nPossible causes:')
    print('  - Invalid API key')
    print('  - IP address not whitelisted')
    print('  - Client disabled on server')
    print('\n')
    sys.exit(1)

except Exception as e:
    print('\n' + '=' * 80)
    print('CONNECTION FAILED')
    print('=' * 80)
    print(f'\nError: {e}')
    print(f'Error type: {type(e).__name__}')
    print('\nPossible causes:')
    print('  - Server not running')
    print('  - Incorrect server address/port')
    print('  - Network/firewall blocking connection')
    print('\n')
    sys.exit(1)