import asyncio
import os
from enum import StrEnum
from .constants import CANVAS_TABLES

from dap.actions.init_db import init_db
from dap.actions.sync_db import sync_db
from dap.api import DAPClient
from dap.dap_types import Credentials
from dap.integration.database_errors import NonExistingTableError
import sys


class SyncTableResult(StrEnum):
    INIT_NEEDED = "init_needed"
    INIT_NOT_NEEDED = "init_not_needed"


class InitTableResult(StrEnum):
    COMPLETED = "completed"
    FAILED = "failed"


db_location = sys.argv[1]
if db_location != "supabase" or db_location != "local":
    print("Invalid argument. Enter 'supabase' or 'local'.")
    exit(1)

api_url = os.environ.get("DAP_API_URL")
dap_client_id = os.environ.get("DAP_CLIENT_ID")
dap_client_secret = os.environ.get("DAP_CLIENT_SECRET")
dap_connection_string = (
    os.environ.get("DAP_CONNECTION_STRING_SUPABASE")
    if db_location == "supabase"
    else os.environ.get("DAP_CONNECTION_STRING_LOCAL")
)
namespace = "canvas"


async def main():
    credentials = Credentials.create(
        client_id=dap_client_id, client_secret=dap_client_secret
    )

    for t in CANVAS_TABLES:
        result = await async_sync_table(
            t, api_url, credentials, namespace, dap_connection_string
        )
        if result == SyncTableResult.INIT_NEEDED:
            result == await async_init_table(
                t, api_url, credentials, namespace, dap_connection_string
            )
            if result == InitTableResult.FAILED:
                print(f"{t} init failed.")
            else:
                print(f"{t} init successfull.")
        else:
            print(f"{t} sync successfull.")


async def async_get_tables(api_url: str, credentials: Credentials, namespace: str):
    async with DAPClient(
        base_url=api_url,
        credentials=credentials,
    ) as session:
        return await session.get_tables(namespace=namespace)


async def async_sync_table(
    table: str,
    api_url: str,
    credentials: Credentials,
    namespace: str,
    connection_string: str,
):
    result = SyncTableResult.INIT_NOT_NEEDED
    try:
        await sync_db(
            base_url=api_url,
            namespace=namespace,
            table_name=table,
            credentials=credentials,
            connection_string=connection_string,
        )
    except NonExistingTableError as e:
        result = SyncTableResult.INIT_NEEDED

    return result


async def async_init_table(
    table: str,
    api_url: str,
    credentials: Credentials,
    namespace: str,
    connection_string: str,
):
    result = InitTableResult.COMPLETED
    try:
        await init_db(
            base_url=api_url,
            namespace=namespace,
            table_name=table,
            credentials=credentials,
            connection_string=connection_string,
        )
    except Exception as e:
        result = InitTableResult.FAILED

    return result


asyncio.run(main())
