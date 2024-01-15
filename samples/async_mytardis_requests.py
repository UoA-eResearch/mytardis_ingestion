import asyncio
import os
import time

import aiohttp
import requests
import tqdm


async def async_main() -> None:
    base_url: str = "https://test-instruments.nectar.auckland.ac.nz"
    api_version: str = "api/v1"
    api_endpoint: str = "dataset_file"

    url = base_url + "/" + api_version + "/" + api_endpoint + "/"

    username: str = os.environ["ASYNC_REQUEST_EXAMPLE_USERNAME"]
    api_key: str = os.environ["ASYNC_REQUEST_EXAMPLE_API_KEY"]

    headers = {
        "Accept": "application/json",
        "Content-Type": "application/json",
        "User-Agent": "Sample Async Request Script",
        "Authorization": f"ApiKey {username}:{api_key}",
    }

    num_requests = 500

    resource_id: int = 18000

    ################################################################################
    # Async requests

    # progress_send = tqdm.tqdm("Sent", total=num_requests, position=1)
    # progress_response = tqdm.tqdm("Responded", total=num_requests, position=0)

    # text = []

    async def get_text(session: aiohttp.ClientSession, resource_id: int) -> str:
        full_url = url + str(resource_id)
        async with session.get(
            url=full_url,
            headers=headers,
            timeout=50,
        ) as response:
            return await response.text()

    async with aiohttp.ClientSession(
        connector=aiohttp.TCPConnector(limit=200)
    ) as session:
        start = time.perf_counter()

        coroutines = []

        for i in range(num_requests):
            resource_id += 1
            # progress_send.update(1)
            coroutines.append(get_text(session, resource_id))
            # print("Async request ", i, ", status=", async_response.status)

            # progress_response.update(1)

        results = await asyncio.gather(*coroutines)

    # assert len(text) == 500
    print("Finished async requests in ", time.perf_counter() - start)


def sequential_main() -> None:
    ################################################################################
    # Sequential requests

    base_url: str = "https://test-instruments.nectar.auckland.ac.nz"
    api_version: str = "api/v1"
    api_endpoint: str = "dataset_file"

    url = base_url + "/" + api_version + "/" + api_endpoint + "/"

    username: str = os.environ["ASYNC_REQUEST_EXAMPLE_USERNAME"]
    api_key: str = os.environ["ASYNC_REQUEST_EXAMPLE_API_KEY"]

    headers = {
        "Accept": "application/json",
        "Content-Type": "application/json",
        "User-Agent": "Sample Async Request Script",
        "Authorization": f"ApiKey {username}:{api_key}",
    }

    num_requests = 500

    resource_id: int = 18000

    # text = []

    with requests.Session() as session:
        start = time.perf_counter()
        # for i in tqdm.tqdm(range(num_requests)):
        for i in range(num_requests):
            full_url = url + str(resource_id)
            resource_id += 1
            sync_response = session.get(
                url=full_url,
                headers=headers,
                timeout=5,
            )
            print("Sequential request ", i, ", status=", sync_response.status_code)
            # text.append(sync_response.text)
            # print("Got response:\n", text)

    # assert len(text) == 500
    print("Finished sequential requests in ", time.perf_counter() - start)


if __name__ == "__main__":
    asyncio.run(async_main())
    sequential_main()
