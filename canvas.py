import asyncio
import os
from enum import StrEnum
from dap.actions.init_db import init_db
from dap.actions.sync_db import sync_db
from dap.api import DAPClient
from dap.dap_types import Credentials
from dap.integration.database_errors import NonExistingTableError
import sys


class SyncTableResult(StrEnum):
    INIT_NEEDED = "init_needed"
    COMPLETED = "completed"
    FAILED = "failed"
    NO_TABLE = "no_table"


class InitTableResult(StrEnum):
    COMPLETED = "completed"
    FAILED = "failed"


canvas_tables = [
    "accounts",
    "pseudonyms",
    "users",
    "courses",
    "assignments",
    "course_sections",
    "enrollments",
]


if len(sys.argv) < 2:
    print("argument missing, enter 'supabase' or 'local'.")
    exit(1)

db_location = sys.argv[1]

if db_location != "supabase" and db_location != "local":
    print("invalid argument, enter 'supabase' or 'local'.")
    exit(1)

base_url = os.environ.get("DAP_API_URL")
dap_client_id = os.environ.get("DAP_CLIENT_ID")
dap_client_secret = os.environ.get("DAP_CLIENT_SECRET")

connection_string = (
    os.environ.get("DAP_CONNECTION_STRING_SUPABASE")
    if db_location == "supabase"
    else os.environ.get("DAP_CONNECTION_STRING_LOCAL")
)

namespace = "canvas"


async def main():
    credentials = Credentials.create(
        client_id=dap_client_id, client_secret=dap_client_secret
    )

    tasks = [
        sync_or_init_table(
            base_url=base_url,
            namespace=namespace,
            table_name=table_name,
            credentials=credentials,
            connection_string=connection_string,
        )
        for table_name in canvas_tables
    ]

    await asyncio.gather(*tasks)


async def async_get_tables(base_url: str, credentials: Credentials, namespace: str):
    async with DAPClient(
        base_url=base_url,
        credentials=credentials,
    ) as session:
        return await session.get_tables(namespace=namespace)


async def sync_table(
    base_url: str,
    namespace: str,
    table_name: str,
    credentials: Credentials,
    connection_string: str,
):
    result = SyncTableResult.COMPLETED
    try:
        await sync_db(
            base_url=base_url,
            namespace=namespace,
            table_name=table_name,
            credentials=credentials,
            connection_string=connection_string,
        )

    except NonExistingTableError as e:
        result = SyncTableResult.NO_TABLE

    except ValueError as e:
        if "table not initialized" in str(e):
            result = SyncTableResult.INIT_NEEDED

    except Exception as e:
        print(f"{table_name} sync_table exception: {e}")
        result = SyncTableResult.FAILED

    return result


async def init_table(
    base_url: str,
    namespace: str,
    table_name: str,
    credentials: Credentials,
    connection_string: str,
):
    result = InitTableResult.COMPLETED
    try:
        await init_db(
            base_url=base_url,
            namespace=namespace,
            table_name=table_name,
            credentials=credentials,
            connection_string=connection_string,
        )
    except Exception as e:
        print(f"{table_name} init_table exception: {e}")
        result = InitTableResult.FAILED

    return result


async def sync_or_init_table(
    base_url: str,
    namespace: str,
    table_name: str,
    credentials: Credentials,
    connection_string: str,
):
    try:
        result = await sync_table(
            base_url=base_url,
            namespace=namespace,
            table_name=table_name,
            credentials=credentials,
            connection_string=connection_string,
        )

        if result == SyncTableResult.INIT_NEEDED:
            result = await init_table(
                base_url=base_url,
                namespace=namespace,
                table_name=table_name,
                credentials=credentials,
                connection_string=connection_string,
            )

            print(f"{result} init table: {table_name}")

        else:
            print(f"{result} sync table: {table_name}")
    except Exception as e:
        print(f"{table_name} sync_or_init_table exception: {e}")


asyncio.run(main())
