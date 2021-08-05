import logging
import argparse
import sys
import os
import asyncio

from nsync import start

def parse_arguments():
    parser = argparse.ArgumentParser(description="Distributed file syncer")

    parser.add_argument("path", type=str, help="the directory to sync")
    parser.add_argument("--host", type=str, default="localhost", help="the hostname of the current node")
    parser.add_argument("--file-server-port", type=int, required=True, help="the port on which to serve files")
    parser.add_argument("--cluster-port", type=int, required=True, help="the port on which to operate the syncronization of the cluster state")
    parser.add_argument("--cluster", type=str, required=True, help="comma-separated list of cluster members in a form of <HOST>:<CLUSTER_PORT>:<FILE_SERVER_PORT>")

    args = parser.parse_args(sys.argv[1:])

    return args


if __name__ == "__main__":
    args = parse_arguments()

    logging.basicConfig(level="INFO", format=f"{args.cluster_port}: %(message)s")

    cluster_members = args.cluster.split(",")
    path = os.path.realpath(os.path.abspath(args.path))

    start(path, args.host, args.file_server_port, args.cluster_port, cluster_members)

    loop = asyncio.get_event_loop()
    loop.run_forever()
