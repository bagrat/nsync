# Nsync

No, this is not a boys-band, this is a program that when ran on multiple
machines, syncs files inside specified directories.

## Design

Nsync is built with Python (for demo purposes) on leader-follower model with
automatic leader election using an implementation of Raft in Python called
[`PySyncObj`](https://github.com/bakwc/PySyncObj) to maintain a shared state
of the cluster about modified files list.

Whenever a file is modified or deleted on a node, it is announced on the cluster
state along with the list of nodes it is already synced to, originally starting
with the source node, e.g.:

```
{
    ...

    "filename": {
        "event": "modified",
        "synced_to": ["node1"]
    }

    ...
}
```

A worker on each node monitors the cluster state, and whenever a file
event is detected that has not been synced to the current node, the node
downloads (or deletes) the file over HTTP (because of task time limit) from one of the other
nodes the file is already synced to. After this is done, the current node is
added to the `synced_to` list on the cluster state.

The same worker on a leader node cleans up the files on the cluster state that
have been synced to all the nodes in the cluster.

This design allows any node to go down at any point, and then come back online
and catch up with the state. Initial sync of files is not implemented due to
time limit, the nodes are assumed to start on directories in identical state (or
empty).

Simultaneous writes are handled through a distributed lock implemented in the
same `PySyncObj` library. Whichever node happens to acquire the lock first, gets
to announce the update to the file. The competing node picks up the event of the
other node and overwrites the updated file.

## Usage

Please follow the steps below to use Nsync:

* First clone the repository:

  ```
  $ git clone https://github.com/bagrat/nsync.git
  ```

* Install dependencies:

  ```
  $ cd nsync
  $ pipenv install --dev
  ```

* Optional: run tests

  ```
  $ pipenv run py.test -v
  ```

* Run multiple instances (check out help with `--help`):

  ```
  $ pipenv run python nsync <YOUR_PATH_1> --file-server-port 8001 --cluster-port 10001 --cluster localhost:10002:8002,localhost:10003:8003
  $ pipenv run python nsync <YOUR_PATH_2> --file-server-port 8002 --cluster-port 10002 --cluster localhost:10001:8001,localhost:10003:8003
  $ pipenv run python nsync <YOUR_PATH_3> --file-server-port 8003 --cluster-port 10003 --cluster localhost:10001:8001,localhost:10002:8002
  ```

## How to make it fast?

First and foremost, rewrite it in a language faster than Python. Additionally
the file sharing could be done on the TCP level, rather than HTTP.
