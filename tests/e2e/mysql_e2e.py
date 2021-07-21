#!/usr/bin/env python3

import mysql.connector as mysql

import memgraph

import subprocess
import pathlib
import atexit

SCRIPT_DIR = pathlib.Path(__file__).resolve().parent
PROJECT_DIR = SCRIPT_DIR.parents[1]
BUILD_DIR = PROJECT_DIR.joinpath("build")

MYSQL_HOST = '127.0.0.1'
MYSQL_USERNAME = 'root'
MYSQL_PASSWORD = 'mysql'
MYSQLX_PORT = 33060
MYSQL_PORT = 3306


def setup_mysql():
    conn = mysql.connect(
        host=MYSQL_HOST,
        user=MYSQL_USERNAME,
        password=MYSQL_PASSWORD,
        port=MYSQL_PORT,
        auth_plugin='mysql_native_password')
    cursor = conn.cursor()
    cursor.execute("CREATE DATABASE imdb")

    with open('dataset/imdb_mysql.sql', 'r') as dump:
        subprocess.run(['docker',
                        'exec',
                        '-i',
                        'mysql',
                        'mysql',
                        f'--user={MYSQL_USERNAME}',
                        f'--password={MYSQL_PASSWORD}',
                        'imdb'],
                       check=True,
                       stdin=dump)


def teardown_mysql():
    conn = mysql.connect(
        host=MYSQL_HOST,
        user=MYSQL_USERNAME,
        password=MYSQL_PASSWORD,
        auth_plugin='mysql_native_password')
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

    print("Preparing MySQL")
    setup_mysql()
    atexit.register(teardown_mysql)

    print("Migrating data from MySQL to Memgraph")
    subprocess.run([str(BUILD_DIR / 'src/mg_migrate'),
                    '--source-kind=mysql',
                    '--source-host',
                    MYSQL_HOST,
                    '--source-port',
                    str(MYSQLX_PORT),
                    '--source-username',
                    MYSQL_USERNAME,
                    '--source-password',
                    MYSQL_PASSWORD,
                    '--source-database=imdb',
                    '--destination-host',
                    memgraph.MEMGRAPH_DESTINATION_HOST,
                    '--destination-port',
                    str(memgraph.MEMGRAPH_DESTINATION_PORT),
                    '--destination-use-ssl=false',
                    ], check=True, stderr=subprocess.STDOUT)
    print("Migration done")

    print("Validating Memgraph data")
    memgraph.validate_imdb(True)
    print("Validation passed")
