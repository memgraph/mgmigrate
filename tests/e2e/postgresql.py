#!/usr/bin/env python3

import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

import mgclient

import subprocess
import os
import atexit

SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))
PROJECT_DIR = os.path.normpath(os.path.join(SCRIPT_DIR, "..", ".."))
BUILD_DIR = os.path.join(PROJECT_DIR, "build")

POSTGRES_HOST = '127.0.0.1'
POSTGRES_USERNAME = 'postgres'
POSTGRES_PASSWORD = 'postgres'
POSTGRES_PORT = 5432

MEMGRAPH_HOST = '127.0.0.1'
MEMGRAPH_PORT = 7687


def SetupPostgres():
    conn = psycopg2.connect(
        host=POSTGRES_HOST, user=POSTGRES_USERNAME, password=POSTGRES_PASSWORD)
    conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
    cursor = conn.cursor()
    cursor.execute("CREATE DATABASE imdb")

    with open('dataset/imdb_postgresql_dump.sql', 'r') as dump:
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


def TeardownPostgres():
    con = psycopg2.connect(
        host=POSTGRES_HOST, user=POSTGRES_USERNAME, password=POSTGRES_PASSWORD)
    con.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
    cursor = con.cursor()
    cursor.execute("DROP DATABASE imdb")

def CleanMemgraph():
    conn = mgclient.connect(host=MEMGRAPH_HOST, port=MEMGRAPH_PORT)
    conn.autocommit = True
    cursor = conn.cursor()
    cursor.execute("MATCH (n) DETACH DELETE n;")
    cursor.fetchall()

    cursor.execute("MATCH (n) RETURN COUNT(n);")
    row = cursor.fetchone()
    assert len(row) == 1 and row[0] == 0, "Failed to clear Memgraph"
    assert not cursor.fetchone()

    # remove all the constraints
    cursor.execute("SHOW CONSTRAINT INFO")
    rows = cursor.fetchall()

    for constraint_type, label, properties in rows:
        if constraint_type == 'exists':
            cursor.execute(f'DROP CONSTRAINT ON (n:{label}) ASSERT exists (n.{properties})')
            cursor.fetchall()
        else:
            assert constraint_type == 'unique', "Invalid constraint type"
            property_list = ', '.join([f'n.{p}' for p in properties])
            cursor.execute(f'DROP CONSTRAINT ON (n:{label}) ASSERT {property_list} IS UNIQUE')
            cursor.fetchall()

    cursor.execute("SHOW CONSTRAINT INFO")
    rows = cursor.fetchall()
    assert len(rows) == 0, "Failed to clean constraints"


def Validate():
    conn = mgclient.connect(host=MEMGRAPH_HOST, port=MEMGRAPH_PORT)
    conn.autocommit = True
    cursor = conn.cursor()

    cursor.execute(
        'MATCH (a:actors {name: "Christian Bale"})-[:movie_roles]->(m:movies) '
        'WHERE m.rating >= 8 '
        'RETURN m.title, m.rating '
        'ORDER BY m.rating DESC')
    rows = cursor.fetchall()
    expected_rows = [
        ('The Dark Knight',
         9.0),
        ('The Prestige',
         8.5),
        ('The Dark Knight Rises',
         8.4),
        ('Batman Begins',
         8.2),
        ('Ford v Ferrari',
         8.1)]
    assert rows == expected_rows, "Invalid results returned"

    cursor.execute(
        'MATCH path = (:actors {name: "George Clooney"})-[* bfs]-(:actors {name: "Kevin Bacon"}) '
        'RETURN size(path) / 2')
    row = cursor.fetchone()
    assert len(row) == 1 and row[0] == 2, "Got invalid path from Memgraph"
    assert not cursor.fetchone()

    cursor.execute(
        'SHOW CONSTRAINT INFO')
    rows = cursor.fetchall()
    expected_rows = [
        ('exists',
         'actors',
         'actor_id'),
        ('exists',
         'actors',
         'name'),
        ('exists',
         'movies',
         'movie_id'),
        ('exists',
         'movies',
         'title'),
        ('exists',
         'tvseries',
         'series_id'),
        ('exists',
         'tvseries',
         'title'),
        ('exists',
         'tvepisodes',
         'series_id'),
        ('exists',
         'tvepisodes',
         'episode_id'),
        ('unique',
         'actors',
         ['actor_id']),
        ('unique',
         'movies',
         ['movie_id']),
        ('unique',
         'tvseries',
         ['series_id']),
        ('unique',
         'tvepisodes',
         ['episode_id'])]
    assert rows == expected_rows, "Invalid constraints returned"


if __name__ == '__main__':
    print("Preparing Memgraph")
    CleanMemgraph()
    atexit.register(CleanMemgraph)

    print("Preparing Postgres")
    SetupPostgres()
    atexit.register(TeardownPostgres)

    print("Migrating data from Postgres to Memgraph")
    subprocess.run([BUILD_DIR + '/src/mg_migrate',
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
                    MEMGRAPH_HOST,
                    '--destination-port',
                    str(MEMGRAPH_PORT),
                    '--destination-use-ssl=false',
                    ], check=True, stderr=subprocess.STDOUT)
    print("Migration done")

    print("Validating Memgraph data")
    Validate()
    print("Validation passed")
