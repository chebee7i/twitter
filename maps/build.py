#!/usr/bin/env python
# coding: utf-8

from __future__ import unicode_literals

import configparser
import os
import errno
import subprocess

from jinja2 import Template, FileSystemLoader
from jinja2.environment import Environment

MAPSPATH = os.path.split(os.path.realpath(__file__))[0]

env = Environment()
env.loader = FileSystemLoader(os.path.join(MAPSPATH, 'templates'))

def mkdir_p(path):
    try:
        os.makedirs(path)
    except OSError as exc: # Python >2.5
        if exc.errno == errno.EEXIST and os.path.isdir(path):
            pass
        else:
            raise

def get_google_key():
    config = configparser.ConfigParser()
    config.read(os.path.join(MAPSPATH, '../project.cfg'))
    googlekey = config['Google Maps']['key']
    return googlekey

def main():
    build_dir = 'build'
    build_dir = os.path.join(MAPSPATH, build_dir)
    mkdir_p(build_dir)
    googlekey = get_google_key()

    filenames = ['grid.html', 'counties.html', 'path.html', 'states.html',
                 'squares.html']
    for filename in filenames:
        template = env.get_template(filename)
        with open(os.path.join(build_dir, filename), 'w') as f:
            f.write(template.render(googlekey=googlekey))

    data_dir = os.path.join(MAPSPATH, 'data')
    subprocess.call('cp -r {} {}/'.format(data_dir, build_dir), shell=True)

if __name__ == '__main__':
    main()
