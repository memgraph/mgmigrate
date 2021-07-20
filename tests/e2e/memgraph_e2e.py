#!/usr/bin/env python3

import memgraph

import mgclient

import subprocess
import os
import atexit

SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))
PROJECT_DIR = os.path.normpath(os.path.join(SCRIPT_DIR, "..", ".."))
BUILD_DIR = os.path.join(PROJECT_DIR, "build")

MEMGRAPH_SOURCE_HOST = '127.0.0.1'
MEMGRAPH_SOURCE_PORT = 7688


def SetupSourceMemgraph():
    conn = mgclient.connect(
        host=MEMGRAPH_SOURCE_HOST,
        port=MEMGRAPH_SOURCE_PORT)
    conn.autocommit = True
    cursor = conn.cursor()
    with open('dataset/imdb_memgraph.cypher', 'r') as dump:
        for query in dump.readlines():
            cursor.execute(query)
            cursor.fetchall()


if __name__ == '__main__':
    print("Preparing destination Memgraph")
    memgraph.CleanMemgraph(
        memgraph.MEMGRAPH_DESTINATION_HOST,
        memgraph.MEMGRAPH_DESTINATION_PORT)
    atexit.register(
        lambda: memgraph.CleanMemgraph(
            memgraph.MEMGRAPH_DESTINATION_HOST,
            memgraph.MEMGRAPH_DESTINATION_PORT))

    print("Preparing source Memgraph")
    memgraph.CleanMemgraph(MEMGRAPH_SOURCE_HOST, MEMGRAPH_SOURCE_PORT)
    atexit.register(
        lambda: memgraph.CleanMemgraph(
            MEMGRAPH_SOURCE_HOST,
            MEMGRAPH_SOURCE_PORT))
    SetupSourceMemgraph()

    print("Migrating data from Memgraph to Memgraph")
    subprocess.run([BUILD_DIR + '/src/mg_migrate',
                    '--source-kind=memgraph',
                    '--source-host',
                    MEMGRAPH_SOURCE_HOST,
                    '--source-port',
                    str(MEMGRAPH_SOURCE_PORT),
                    '--source-use-ssl=false',
                    '--destination-host',
                    memgraph.MEMGRAPH_DESTINATION_HOST,
                    '--destination-port',
                    str(memgraph.MEMGRAPH_DESTINATION_PORT),
                    '--destination-use-ssl=false',
                    ], check=True, stderr=subprocess.STDOUT)
    print("Migration done")

    print("Validating Memgraph data")
    memgraph.ValidateImdb(False)
    print("Validation passed")
