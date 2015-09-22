#!/usr/bin/env python3

from setuptools import setup


def parse_requirements():
    requirements = []
    with open('requirements.txt') as f:
        for line in f.readlines():
            line = line.strip()
            if not line or line.startswith('#'):
                continue
            requirements.append(line)

    return requirements


setup(
    name='swh.core',
    description='Software Heritage core utilities',
    author='Software Heritage developers',
    author_email='swh-devel@inria.fr',
    url='https://forge.softwareheritage.org/diffusion/DCORE/',
    packages=['swh.core', 'swh.core.tests'],
    scripts=['bin/swh-hashdir', 'bin/swh-hashfile'],
    install_requires=parse_requirements(),
    setup_requires=['vcversioner'],
    vcversioner={},
    include_package_data=True,
)
