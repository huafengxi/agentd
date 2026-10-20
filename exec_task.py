#!/usr/bin/env python3
"""Exec the registered shell command; S retains process/group/wait ownership."""
import base64
import os
import sys


def main():
    if len(sys.argv)!=2:raise SystemExit('one encoded registered command required')
    command=base64.b64decode(sys.argv[1],validate=True).decode('utf-8')
    if '\x00' in command:raise ValueError('NUL in registered command')
    # The fixed Git source supplies the launcher; no extra long-lived wrapper.
    os.execvpe('bash',['bash','-c',command],os.environ)


if __name__=='__main__':main()
