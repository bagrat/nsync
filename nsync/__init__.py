import asyncio
import logging 
import os
import sys
import random

from pysyncobj import SyncObj, replicated
from pysyncobj.batteries import ReplDict, ReplLockManager
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
from aiohttp import web, ClientSession
import aiohttp


logger = logging.getLogger(__name__)


class PendingFilesMonitor(FileSystemEventHandler):
    def __init__(self, path, cluster_manager):
        super().__init__()

        self._path = path
        self._cluster_manager = cluster_manager
        self._files_to_sync = {}

    def _get_relative_path(self, abs_path):
        prefix_length = len(self._path) + 1

        return abs_path[prefix_length:]

    def on_any_event(self, event):
        if event.event_type not in ("modified", "deleted"):
            return

        if event.is_directory:
            return

        if event.event_type == "modified" and not os.path.exists(event.src_path):
            # Watchdog emits "modified" prior to "deleted" event
            return

        relative_path = self._get_relative_path(event.src_path)

        if relative_path in self._files_to_sync:
            return

        logger.info("%s got %s", event.src_path, event.event_type)

        self._cluster_manager.try_announce_update(relative_path, event.event_type)

    async def _download_file(self, filename, sources):
        source_node = random.choice(sources)
        file_server_address = self._cluster_manager._nodes[source_node].file_server_address

        logger.info("Going to download %s from %s", filename, file_server_address)

        try:
            async with ClientSession() as session:
                async with session.get(f"http://{file_server_address}/{filename}") as resp:
                    if resp.status != 200:
                        await self._download_file(filename, sources)
                        return

                    contents = await resp.text()

                    filepath = os.path.join(self._path, filename)
                    directory = os.path.dirname(filepath)
                    os.makedirs(directory, exist_ok=True)
                    os.chmod(directory, 0o777)

                    with open(filepath, "w") as file:
                        file.write(contents)

                    logger.info("Downloaded %s from %s", filename, file_server_address)

        except aiohttp.client_exceptions.ClientConnectorError:
            await self._download_file(filename, sources)

    def _delete_file(self, filename):
        filepath = os.path.join(self._path, filename)
        if os.path.exists(filepath):
            os.remove(filepath)

    async def _monitor_pending_files(self):
        while True:
            try:
                self._cluster_manager.cleanup()

                logger.info("state: %s", self._cluster_manager._pending_files)

                self._files_to_sync = self._cluster_manager.get_files_to_sync()
                logger.info("Files to sync: %s", self._files_to_sync)

                for filename, state in self._files_to_sync.items():
                    if state["event"] == "modified":
                        await self._download_file(filename, state["synced_to"])
                        self._cluster_manager.announce_acquisition(filename)

                    if state["event"] == "deleted":
                        self._delete_file(filename)

            finally:
                await asyncio.sleep(1)

    def file_is_being_synced(self, filename):
        return filename in self._files_to_sync

    def start(self):
        loop = asyncio.get_event_loop()
        loop.create_task(self._monitor_pending_files())

        observer = Observer()
        observer.schedule(self, path=self._path, recursive=True)
        observer.start()


class Node(object):
    def __init__(self, host, cluster_port, file_server_port):
        super().__init__()

        self.host = host
        self.cluster_port = cluster_port
        self.file_server_port = file_server_port

    @property
    def cluster_address(self):
        return f"{self.host}:{self.cluster_port}"

    @property
    def file_server_address(self):
        return f"{self.host}:{self.file_server_port}"


class ClusterManager(SyncObj):
    def __init__(self, me, nodes):
        lock = ReplLockManager(autoUnlockTime=5)

        member_addresses = [n.cluster_address for n in nodes]
        super().__init__(me.cluster_address, member_addresses, consumers=[lock])

        self._lock = lock
        self._address = me.cluster_address

        self._nodes = {}
        for node in nodes:
            self._nodes[node.cluster_address] = node

        self._pending_files = {}

    def get_files_to_sync(self):
        files_to_sync = {}
        for filename in self._pending_files:
            if self._address not in self._pending_files[filename]["synced_to"]:
                files_to_sync[filename] = self._pending_files[filename]

        return files_to_sync

    def _file_is_synced(self, filename):
        return filename not in self._pending_files or \
            self._address in self._pending_files[filename]["synced_to"]

    def try_announce_update(self, filename, event):
        logger.info(f"Will try to acquire lock for {filename}")

        if self._lock.tryAcquire(f"announce:{filename}", sync=True):
            if self._file_is_synced(filename):
                logger.info(f"Announcing update to {filename}")

                while True:
                    logger.info("Will try to acquire cleanup lock")

                    if self._lock.tryAcquire(f"cleanup:{filename}", sync=True):
                        self._announce_update(filename, self._address, event)

                        self._lock.release(f"cleanup:{filename}")
                        break

            self._lock.release(f"announce:{filename}")

    @replicated
    def _announce_update(self, filename, source, event):
        self._pending_files[filename] = {
            "event": event,
            "synced_to": [source]
        }

    def announce_acquisition(self, filename):
        self._announce_acquisition(filename, self._address)

    @replicated
    def _announce_acquisition(self, filename, node_acquired):
        self._pending_files[filename]["synced_to"].append(node_acquired)

    def cleanup(self):
        leader = str(self._getLeader())
        if leader == self._address:
            logger.info("Running cleanup")

            for filename, state in self._pending_files.copy().items():
                if len(state["synced_to"]) == len(self._nodes) + 1:
                    if self._lock.tryAcquire(f"cleanup:{filename}", sync=True):
                        self._try_clean_file_record(filename)
                        
                        logger.info("Releasing cleanup lock for %s", filename)
                        self._lock.release(f"cleanup:{filename}")

    @replicated
    def _try_clean_file_record(self, filename):
        if filename in self._pending_files:
            del self._pending_files[filename]


async def start_file_server(host, port, path):
    app = web.Application()
    app.router.add_static("/", path)

    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, host, port)
    await site.start()


def start(path, host, file_server_port, cluster_port, cluster_members):
    my_address = f"{host}:{cluster_port}"

    me = Node(host, cluster_port, file_server_port)
    nodes = []
    for member in cluster_members:
        try:
            host, cluster_port, file_server_port = member.split(":")
            nodes.append(Node(host, cluster_port, file_server_port))
        except ValueError:
            logger.error("Bad node address: %s", member)

    cluster_manager = ClusterManager(me, nodes)

    loop = asyncio.get_event_loop()
    loop.create_task(start_file_server(host, me.file_server_port, path))

    pending_files_monitor = PendingFilesMonitor(path, cluster_manager)
    pending_files_monitor.start()
