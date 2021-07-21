#!/usr/bin/env python3

import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

import memgraph

import subprocess
import pathlib
import atexit

SCRIPT_DIR = pathlib.Path(__file__).resolve().parent
PROJECT_DIR = SCRIPT_DIR.parents[1]
BUILD_DIR = PROJECT_DIR.joinpath("build")

POSTGRES_HOST = '127.0.0.1'
POSTGRES_USERNAME = 'postgres'
POSTGRES_PASSWORD = 'postgres'
POSTGRES_PORT = 5432


def setup_postgres():
    conn = psycopg2.connect(
        host=POSTGRES_HOST, user=POSTGRES_USERNAME, password=POSTGRES_PASSWORD)
    conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
    cursor = conn.cursor()
    cursor.execute("CREATE DATABASE imdb")

    with open('dataset/imdb_postgresql.sql', 'r') as dump:
        subprocess.run(['docker',
                        'exec',
                        '-i',
                        'postgres',
                        'psql',
                        '-U',
                        POSTGRES_USERNAME,
                        '-h',
                        POSTGRES_HOST,
                        '-d',
                        'imdb'],
                       check=True,
                       stdin=dump)


def teardown_postgres():
    conn = psycopg2.connect(
        host=POSTGRES_HOST, user=POSTGRES_USERNAME, password=POSTGRES_PASSWORD)
    conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
    cursor = conn.cursor()
    cursor.execute("DROP DATABASE imdb")


if __name__ == '__main__':
    print("Preparing Memgraph")
    memgraph.clean_memgraph(
        memgraph.MEMGRAPH_DESTINATION_HOST,
        memgraph.MEMGRAPH_DESTINATION_PORT)
    atexit.register(
        lambda: memgraph.clean_memgraph(
            memgraph.MEMGRAPH_DESTINATION_HOST,
            memgraph.MEMGRAPH_DESTINATION_PORT))

    print("Preparing Postgres")
    setup_postgres()
    atexit.register(teardown_postgres)

    print("Migrating data from Postgres to Memgraph")
    subprocess.run([str(BUILD_DIR / 'src/mg_migrate'),
                    '--source-kind=postgresql',
                    '--source-host',
                    POSTGRES_HOST,
                    '--source-port',
                    str(POSTGRES_PORT),
                    '--source-username',
                    POSTGRES_USERNAME,
                    '--source-password',
                    POSTGRES_PASSWORD,
                    '--source-database=imdb',
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
