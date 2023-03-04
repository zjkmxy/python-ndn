import aiohttp
import json
import asyncio
from .. import encoding as enc


# http based
# send request to server with json
# parse json


class GqlClient:
    def __init__(self, gqlserver: str):
        self.address = gqlserver

    async def _request(self, query: str, request: dict):
        async with aiohttp.ClientSession() as session:
            async with session.post(self.address, data=json.dumps({
                "query": query,
                "variables": request,
            })) as response:
                # add some error handling here, probably
                return await response.json()

    async def create_face(self, locator: dict) -> str:
        response = await self._request("mutation createFace($locator: JSON!) { createFace(locator: $locator) { id }}",
                                       {"locator": locator})
        if "data" not in response:
            raise RuntimeError(json.dumps(response))
        return response["data"]["createFace"]["id"]

    async def insert_fib(self, face: str, prefix: str):
        response = await self._request(
            """mutation insertFibEntry($name: Name!, $nexthops: [ID!]!, $strategy: ID, $params: JSON) {
                    insertFibEntry(name: $name, nexthops: $nexthops, strategy: $strategy, params: $params) {
                        id
                    }
                }
            """, {"name": prefix, "nexthops": [face]})
        return response["data"]["insertFibEntry"]["id"]

    async def delete(self, id: str) -> bool:
        response = await self._request("mutation delete($id: ID!) {delete(id: $id)}",
                                       {"id": id})
        return response["data"]["delete"]
