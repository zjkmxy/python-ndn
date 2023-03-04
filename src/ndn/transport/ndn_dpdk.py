# -----------------------------------------------------------------------------
# Copyright (C) 2019-2022 The python-ndn authors
#
# This file is part of python-ndn.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
# -----------------------------------------------------------------------------
from abc import ABC
import asyncio as aio
import logging
from .face import Face
from .prefix_registerer import PrefixRegisterer
from .graphql import GqlClient
from .. import encoding as enc
from ..encoding import Name


class NdnDpdkFace(Face, ABC):
    face_id: str
    gql_url: str
    client: GqlClient

    def __init__(self, gql_url: str):
        super().__init__()
        self.gql_url = gql_url
        self.face_id = ""
        self.client = GqlClient(gql_url)

    async def isLocalFace(self):
        return False


class NdnDpdkUdpFace(NdnDpdkFace):
    self_addr: str
    self_port: int
    dpdk_addr: str
    dpdk_port: int

    def __init__(self, gql_url: str, self_addr: str, self_port: int,
                 dpdk_addr: str, dpdk_port: int):
        super().__init__(gql_url)
        self.self_addr = self_addr
        self.self_port = self_port
        self.dpdk_addr = dpdk_addr
        self.dpdk_port = dpdk_port
        self.transport = None
        self.handler = None
        self.close = None

    async def open(self):
        class PacketHandler:
            def __init__(self, callback, close) -> None:
                self.callback = callback
                self.close = close
                self.transport = None

            def connection_made(self, transport: aio.DatagramTransport):
                self.transport = transport

            def datagram_received(self, data: bytes, _addr: tuple[str, int]):
                typ, _ = enc.parse_tl_num(data)
                aio.create_task(self.callback(typ, data))

            def send(self, data):
                self.transport.sendto(data)

            def error_received(self, exc: Exception):
                self.close.set_result(True)
                logging.warning(exc)

            def connection_lost(self, exc):
                if not self.close.done():
                    self.close.set_result(True)
                if exc:
                    logging.warning(exc)

        # Start UDP listener
        loop = aio.get_running_loop()
        self.running = True
        close = loop.create_future()
        handler = PacketHandler(self.callback, close)
        transport, _ = await loop.create_datagram_endpoint(
            lambda: handler,
            local_addr=(self.self_addr, self.self_port),
            remote_addr=(self.dpdk_addr, self.dpdk_port))
        self.handler = handler
        self.transport = transport
        self.close = close

        # Send GraphQL command
        # TODO: IPv6 is not supported.
        self.face_id = await self.client.create_face({
            "scheme": "udp",
            "remote": f'{self.self_addr}:{self.self_port}',
            "local": f'{self.dpdk_addr}:{self.dpdk_port}',
        })

    def shutdown(self):
        self.running = False

        # Send GraphQL command
        aio.create_task(self.client.delete(self.face_id))

        if self.transport is not None:
            self.transport.close()
        self.face_id = ""

    def send(self, data: bytes):
        if self.handler is not None:
            self.handler.send(data)
        else:
            raise ValueError('Unable to send packet before connection')

    async def run(self):
        await self.close


class DpdkRegisterer(PrefixRegisterer):
    face: NdnDpdkFace
    fib_entries: dict[str, str]

    def __init__(self, face: NdnDpdkFace):
        super().__init__()
        self.face = face
        self.fib_entries = {}
    
    async def register(self, name: enc.FormalName) -> bool:
        if not self.face.running:
            raise ValueError('Cannot register prefix when face is not running')
        client = self.face.client
        name_uri = Name.to_canonical_uri(name)
        try:
            fib_id = await client.insert_fib(self.face.face_id, name_uri)
            self.fib_entries[name_uri] = fib_id
        except KeyError:
            return False
        return True

    async def unregister(self, name: enc.FormalName) -> bool:
        if not self.face.running:
            raise ValueError('Cannot unregister prefix when face is not running')
        client = self.face.client
        name_uri = Name.to_canonical_uri(name)
        if name_uri not in self.fib_entries:
            return False
        fib_id = self.fib_entries[name_uri]
        try:
            await client.delete(fib_id)
        except KeyError:
            return False
        return True


class NdnDpdkMemifFace(NdnDpdkFace):
    socket_name: str
    id_num: int

    def __init__(self, gql_url: str, memif_class, socket_name: str, id_num: int):
        super().__init__(gql_url)
        self.memif_class = memif_class
        self.socket_name = socket_name
        self.id_num = id_num
        self.memif = None

    async def open(self):
        # Send GraphQL command
        self.face_id = await self.client.create_face({
            "dataroom": 2048,
            "scheme": "memif",
            "id": self.id_num,
            "socketName": self.socket_name,
            "socketOwner": [0, 0],
        })

        self.memif = self.memif_class(self.socket_name, self.id_num, False, self._rx_proc)
        self.running = True

    def _rx_proc(self, data: bytes):
        typ, _ = enc.parse_tl_num(data)
        aio.create_task(self.callback(typ, data))

    async def run(self):
        while self.running:
            self.memif.poll()
            await aio.sleep(0.01)

    def shutdown(self):
        self.running = False

        # Send GraphQL command
        aio.create_task(self.client.delete(self.face_id))

        self.memif = None
        self.face_id = ""

    def send(self, data: bytes):
        if self.memif is not None:
            self.memif.send(data)
        else:
            raise ValueError('Unable to send packet before connection')
