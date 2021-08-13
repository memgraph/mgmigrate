#!/usr/bin/env python3

import argparse
import os
import subprocess
from pathlib import Path
from shutil import copy 

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("source_dir")
    parser.add_argument("install_dir")
    args = parser.parse_args()
    source_dir = Path(args.source_dir)
    os.chdir(source_dir)

    subprocess.run(["./configure", "--with-gssapi=no", "--with-ldap=no"], check=True, stderr=subprocess.STDOUT)

    os.chdir(source_dir / "src/interfaces/libpq")
    subprocess.run(["make"], check=True, stderr=subprocess.STDOUT)
    copy('libpq.a', args.install_dir)

    os.chdir(source_dir / "src/common")
    subprocess.run(["make"], check=True, stderr=subprocess.STDOUT)
    copy('libpgcommon.a', args.install_dir)

    os.chdir(source_dir / "src/port")
    subprocess.run(["make"], check=True, stderr=subprocess.STDOUT)
    copy('libpgport.a', args.install_dir)
