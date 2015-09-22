#!/usr/bin/env python

from setuptools import setup

setup(
    name='swh.core',
    description='Software Heritage core utilities',
    author='Software Heritage developers',
    author_email='swh-devel@inria.fr',
    url='https://forge.softwareheritage.org/diffusion/DCORE/',
    packages=['swh.core', 'swh.core.tests'],
    scripts=['bin/swh-hashdir', 'bin/swh-hashfile'],
    install_requires=open('requirements.txt').read().splitlines(),
    setup_requires=['vcversioner'],
    vcversioner={},
    include_package_data=True,
)
