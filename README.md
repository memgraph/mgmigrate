<h1 align="center">
  mgmigrate
</h1>

<p align="center">
  <a href="https://github.com/memgraph/mgmigrate/LICENSE">
    <img src="https://img.shields.io/github/license/memgraph/mgmigrate" alt="license" title="license"/>
  </a>
  <a href="https://github.com/memgraph/mgmigrate">
    <img src="https://img.shields.io/badge/PRs-welcome-brightgreen.svg" alt="build" title="build"/>
  </a>
  <a href="https://github.com/memgraph/mgmigrate/actions">
    <img src="https://img.shields.io/github/workflow/status/memgraph/mgmigrate/CI" alt="build" title="build"/>
  </a>
</p>

<p align="center">
    <a href="https://twitter.com/intent/follow?screen_name=memgraphdb"><img
    src="https://img.shields.io/twitter/follow/memgraphdb.svg?label=Follow%20@memgraphdb"
    alt="Follow @memgraphdb" /></a>
</p>

Welcome to the **mgmigrate** tool repository. This tool can help you migrate
data from PostgreSQL or MySQL to Memgraph. It can also be used to migrate data
between Memgraph instances.

## ⚙️ Installation guide

You can install mgmigrate on the following systems:

* [Linux instructions](#linux)
* [Windows instructions](#windows)
* [macOS instructions](#macos)

### Linux

To install compile dependencies on **Debian / Ubuntu** run:

```
apt-get install -y git cmake make gcc g++ python3
pip3 install pymgclient psycopg2 mysql-connector-python
```

On **RedHat / CentOS / Fedora** run:

```
yum install -y git cmake make gcc gcc-c++ python3
pip3 install pymgclient psycopg2 mysql-connector-python
```

Once all the requirements are in place, create a build directory inside the
source directory and configure the build by running CMake from it as follows:

```console
mkdir build
cd build
cmake -DCMAKE_BUILD_TYPE=Release -DCMAKE_INSTALL_PREFIX=. ..
```

Continue with the [Building and installing](#building-and-installing) step.

### Windows

Before you can install mgmigrate on Windows, you will need to install the needed
dependencies. On **Windows**, you need to install the MSYS2. Just follow the
[instructions](https://www.msys2.org), up to step 6. In addition, OpenSSL must
be installed. You can easily install it with an
[installer](https://slproweb.com/products/Win32OpenSSL.html). The Win64 version
is required, although the "Light" version is enough. Both EXE and MSI variants
should work. Then, you'll need to install the dependencies using the MSYS2
MINGW64 terminal, which should be available from your Start menu. Just run the
following command inside the MSYS2 MINGW64 terminal:

```console
pacman -Syu --needed base-devel git mingw-w64-x86_64-toolchain mingw-w64-x86_64-cmake mingw-w64-x86_64-openssl mingw-w64-x86_64-postgresql
```

Once all the requirements are in place, create a build directory inside the
source directory and configure the build by running CMake from it as follows:

```console
mkdir build
cd build
cmake -DCMAKE_BUILD_TYPE=Release -G"MSYS Makefiles" -DCMAKE_INSTALL_PREFIX=. ..
```

Continue with the [Building and installing](#building-and-installing) step.

### macOS

On **MacOS**, first make sure you have
[XCode](https://developer.apple.com/xcode/) and [Homebrew](https://brew.sh)
installed. Then, in the terminal, run:

```
brew install git cmake make openssl postgresql
```

Once all the requirements are in place, create a build directory inside the
source directory and configure the build by running CMake from it as follows:

```console
mkdir build && cd build
cmake -DCMAKE_BUILD_TYPE=Release -DOPENSSL_ROOT_DIR="$(ls -rd -- /usr/local/Cellar/openssl@1.1/* | head -n 1)" -DCMAKE_INSTALL_PREFIX=. ..
```

Continue with the [Building and installing](#building-and-installing) step.

### Building and installing
After running CMake, you should see a Makefile in the build directory. Then you
can build the project by running:

```console
make
```

Once all the requirements are in place, create a build directory inside the
source directory and configure the build by running CMake from it as follows:

```console
make install
```

This will install to system default installation directory. If you want to
change this location, use `-DCMAKE_INSTALL_PREFIX` option when running CMake.

## 📋 Usage

Here is an example of how to run the mgmigrate tool:

```console
build/src/mgmigrate --source-kind=postgresql /
  --source-host 127.0.0.1 /
  --source-port 5432 /
  --source-username postgres /
  --source-password postgres /
  --source-database=exampledatabase /
  --destination-host 127.0.0.1 /
  --destination-port 7687 /
  --destination-use-ssl=false
```

The available arguments are:

| Parameter      | Description | Default     |
| -------------- | ----------- | ----------- |
| --source-kind         | The kind of the given server. Supported options are: `memgraph` , `mysql` and `postgresql`. | memgraph
| --source-host         | Server address of the source database. | 127.0.0.1
| --source-port         | Server port of the source database.  | 0
| --source-username     | Username for the source database. | -
| --source-password     | Password for the source database. | -
| --source-database     | Database name. Applicable to PostgreSQL and MySQL source. | -
| --destination-use-ssl | Should the connection to the source database (if Memgraph) use SSL. | false
| --destination-host    | Server address of the destination database. | 127.0.0.1
| --destination-port    | Server port number of the destination database. | 7687
| --destination-username| Username for the destination database. | -
| --destination-password| Password for the destination database. | -
| --destination-use-ssl | Should the connection to the destination database (if Memgraph) use SSL. | false

### Examples

You can find more usage examples in the tests directory:
* [`memgraph_e2e.py`](/tests/e2e/memgraph_e2e.py)
* [`mysql_e2e.py`](/tests/e2e/mysql_e2e.py)
* [`postresql_e2e.py`](/tests/e2e/postgresql_e2e.py)
*