import tempfile
import pytest
import time
import os
import shutil

from subprocess import Popen


class Node(object):
    def __init__(self, name, path, file_server_port, cluster_port, cluster):
        super().__init__()

        self.name = name
        self.path = path
        self.cluster_port = cluster_port
        self.file_server_port = file_server_port
        self.cluster = cluster
        self.process = None

    def start(self):
        cmd = [
            "pipenv",
            "run",
            "python",
            "nsync",
            "--cluster-port",
            str(self.cluster_port),
            "--cluster",
            ",".join(self.cluster),
            "--file-server-port",
            str(self.file_server_port),
            self.path
        ]

        self.process = Popen(cmd, stdin=None, stdout=None, stderr=None)

    def stop(self):
        self.process.kill()


cluster_port1 = 10001
cluster_port2 = 10002
cluster_port3 = 10003

file_server_port1 = 8081
file_server_port2 = 8082
file_server_port3 = 8083


@pytest.fixture
def node1():
    path = tempfile.mkdtemp()
    time.sleep(1)
    node = Node("node1", path, file_server_port1, cluster_port1, [f"localhost:{cluster_port2}:{file_server_port2}", f"localhost:{cluster_port3}:{file_server_port3}"])
    node.start()
    time.sleep(1)
    yield node
    node.stop()


@pytest.fixture
def node2():
    path = tempfile.mkdtemp()
    time.sleep(1)
    node = Node("node2", path, file_server_port2, cluster_port2, [f"localhost:{cluster_port1}:{file_server_port1}", f"localhost:{cluster_port3}:{file_server_port3}"])
    node.start()
    time.sleep(1)
    yield node
    node.stop()


@pytest.fixture
def node3():
    path = tempfile.mkdtemp()
    time.sleep(1)
    node = Node("node3", path, file_server_port3, cluster_port3, [f"localhost:{cluster_port1}:{file_server_port1}", f"localhost:{cluster_port2}:{file_server_port2}"])
    node.start()
    time.sleep(1)
    yield node
    node.stop()


def assert_file_present_on_node(node, filename, contents):
    filepath_on_node = os.path.join(node.path, filename)

    assert os.path.isfile(filepath_on_node), f"File {filename} should be synced to {node.name}"

    with open(filepath_on_node) as file:
        file_contents_on_node = file.read()

        assert file_contents_on_node == contents, f"File {filename} contents should be {contents} on {node.name}"


def assert_file_absent_on_node(node, filename):
    filepath_on_node = os.path.join(node.path, filename)

    assert not os.path.exists(filepath_on_node), f"File {filename} should not be present on {node.name}"


def populate_file_on_node(node, filename, contents):
    with open(os.path.join(node.path, filename), "w") as file1:
        file1.write(contents)


def delete_on_node(node, path):
    fullpath = os.path.join(node.path, path)
    if os.path.isdir(fullpath):
        shutil.rmtree(fullpath)
    else:
        os.remove(fullpath)


def test_basic(node1, node2, node3):
    filename1 = "test-file-1.txt"
    contents1 = "contents1"

    populate_file_on_node(node1, filename1, contents1)

    time.sleep(4)

    assert_file_present_on_node(node1, filename1, contents1)
    assert_file_present_on_node(node2, filename1, contents1)
    assert_file_present_on_node(node3, filename1, contents1)


def test_delete(node1, node2, node3):
    folder1 = "folder1"

    os.makedirs(os.path.join(node1.path, folder1))
    os.makedirs(os.path.join(node2.path, folder1))

    filename1 = os.path.join(folder1, "test-file-1.txt")
    filename2 = os.path.join(folder1, "test-file-2.txt")

    contents1 = "contents1"
    contents2 = "contents2"

    populate_file_on_node(node1, filename1, contents1)
    populate_file_on_node(node2, filename2, contents2)

    time.sleep(4)

    assert_file_present_on_node(node1, filename1, contents1)
    assert_file_present_on_node(node2, filename1, contents1)
    assert_file_present_on_node(node3, filename1, contents1)

    assert_file_present_on_node(node1, filename2, contents2)
    assert_file_present_on_node(node2, filename2, contents2)
    assert_file_present_on_node(node3, filename2, contents2)

    time.sleep(4)

    delete_on_node(node3, folder1)

    time.sleep(4)

    assert_file_absent_on_node(node1, filename1)
    assert_file_absent_on_node(node2, filename1)
    assert_file_absent_on_node(node3, filename1)

    assert_file_absent_on_node(node1, filename2)
    assert_file_absent_on_node(node2, filename2)
    assert_file_absent_on_node(node3, filename2)


def test_different_files_on_different_nodes(node1, node2, node3):
    folder1 = "folder1"
    folder3 = "folder3/subfolder"

    os.makedirs(os.path.join(node1.path, folder1))
    os.makedirs(os.path.join(node2.path, folder1))
    os.makedirs(os.path.join(node3.path, folder3))

    filename1 = os.path.join(folder1, "test-file-1.txt")
    filename2 = os.path.join(folder1, "test-file-2.txt")
    filename3 = os.path.join(folder3, "test-file-3.txt")

    contents1 = "contents1"
    contents2 = "contents2"
    contents3 = "contents3"

    populate_file_on_node(node1, filename1, contents1)
    populate_file_on_node(node2, filename2, contents2)
    populate_file_on_node(node3, filename3, contents3)

    time.sleep(4)

    assert_file_present_on_node(node1, filename2, contents2)
    assert_file_present_on_node(node1, filename3, contents3)

    assert_file_present_on_node(node2, filename1, contents1)
    assert_file_present_on_node(node2, filename3, contents3)

    assert_file_present_on_node(node3, filename1, contents1)
    assert_file_present_on_node(node3, filename2, contents2)


def test_sequential_updates(node1, node2, node3):
    filename1 = "test-file-1.txt"
    contents1 = "contents1"
    contents2 = "contents2"

    populate_file_on_node(node1, filename1, contents1)
    time.sleep(0.4)
    populate_file_on_node(node1, filename1, contents2)

    time.sleep(4)

    assert_file_present_on_node(node1, filename1, contents2)
    assert_file_present_on_node(node2, filename1, contents2)
    assert_file_present_on_node(node3, filename1, contents2)


def test_node_going_down(node1, node2, node3):
    filename1 = "test-file-1.txt"
    contents1 = "contents1"
    contents2 = "contents2"

    populate_file_on_node(node1, filename1, contents1)

    time.sleep(4)

    assert_file_present_on_node(node1, filename1, contents1)
    assert_file_present_on_node(node2, filename1, contents1)
    assert_file_present_on_node(node3, filename1, contents1)

    node2.stop()

    time.sleep(2)

    populate_file_on_node(node3, filename1, contents2)

    time.sleep(4)

    assert_file_present_on_node(node1, filename1, contents2)
    assert_file_present_on_node(node3, filename1, contents2)

    assert_file_present_on_node(node2, filename1, contents1)

    node2.start()

    time.sleep(6)

    assert_file_present_on_node(node2, filename1, contents2)


def test_simultaneous(node1, node2, node3):
    filename1 = "test-file-1.txt"
    contents1 = "contents1"
    contents2 = "contents2"

    populate_file_on_node(node1, filename1, contents1)
    populate_file_on_node(node2, filename1, contents2)

    time.sleep(4)

    try:
        assert_file_present_on_node(node1, filename1, contents2)
        assert_file_present_on_node(node2, filename1, contents2)
        assert_file_present_on_node(node3, filename1, contents2)
    except AssertionError:
        assert_file_present_on_node(node1, filename1, contents1)
        assert_file_present_on_node(node2, filename1, contents1)
        assert_file_present_on_node(node3, filename1, contents1)
