#!/usr/bin/env python3

import memgraph

import mgclient

import subprocess
import pathlib
import atexit

SCRIPT_DIR = pathlib.Path(__file__).resolve().parent
PROJECT_DIR = SCRIPT_DIR.parents[1]
BUILD_DIR = PROJECT_DIR.joinpath("build")

MEMGRAPH_SOURCE_HOST = '127.0.0.1'
MEMGRAPH_SOURCE_PORT = 7688


def setup_source_memgraph():
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
    memgraph.clean_memgraph(
        memgraph.MEMGRAPH_DESTINATION_HOST,
        memgraph.MEMGRAPH_DESTINATION_PORT)
    atexit.register(
        lambda: memgraph.clean_memgraph(
            memgraph.MEMGRAPH_DESTINATION_HOST,
            memgraph.MEMGRAPH_DESTINATION_PORT))

    print("Preparing source Memgraph")
    memgraph.clean_memgraph(MEMGRAPH_SOURCE_HOST, MEMGRAPH_SOURCE_PORT)
    atexit.register(
        lambda: memgraph.clean_memgraph(
            MEMGRAPH_SOURCE_HOST,
            MEMGRAPH_SOURCE_PORT))
    setup_source_memgraph()

    print("Migrating data from Memgraph to Memgraph")
    subprocess.run([str(BUILD_DIR / 'src/mg_migrate'),
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
    memgraph.validate_imdb(False)
    print("Validation passed")
