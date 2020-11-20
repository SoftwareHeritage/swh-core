#!/usr/bin/env python3
# Copyright (C) 2015-2018  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from io import open
import os
from os import path

from setuptools import find_packages, setup

here = path.abspath(path.dirname(__file__))

# Get the long description from the README file
with open(path.join(here, "README.md"), encoding="utf-8") as f:
    long_description = f.read()


def parse_requirements(*names):
    requirements = []
    for name in names:
        if name:
            reqf = "requirements-%s.txt" % name
        else:
            reqf = "requirements.txt"

        if not os.path.exists(reqf):
            return requirements

        with open(reqf) as f:
            for line in f.readlines():
                line = line.strip()
                if not line or line.startswith("#"):
                    continue
                requirements.append(line)
    return requirements


setup(
    name="swh.core",
    description="Software Heritage core utilities",
    long_description=long_description,
    long_description_content_type="text/markdown",
    python_requires=">=3.7",
    author="Software Heritage developers",
    author_email="swh-devel@inria.fr",
    url="https://forge.softwareheritage.org/diffusion/DCORE/",
    packages=find_packages(),
    py_modules=["pytest_swh_core"],
    scripts=[],
    install_requires=parse_requirements(None, "swh"),
    setup_requires=["setuptools-scm"],
    use_scm_version=True,
    extras_require={
        "testing-core": parse_requirements("test"),
        "logging": parse_requirements("logging"),
        "db": parse_requirements("db", "db-pytestplugin"),
        "testing-db": parse_requirements("test-db"),
        "http": parse_requirements("http"),
        # kitchen sink, please do not use
        "testing": parse_requirements(
            "test", "test-db", "db", "db-pytestplugin", "http", "logging"
        ),
    },
    include_package_data=True,
    entry_points="""
        [console_scripts]
        swh=swh.core.cli:main
        swh-db-init=swh.core.cli.db:db_init
        [swh.cli.subcommands]
        db=swh.core.cli.db
        [pytest11]
        pytest_swh_core = swh.core.pytest_plugin
    """,
    classifiers=[
        "Programming Language :: Python :: 3",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: GNU General Public License v3 (GPLv3)",
        "Operating System :: OS Independent",
        "Development Status :: 5 - Production/Stable",
    ],
    project_urls={
        "Bug Reports": "https://forge.softwareheritage.org/maniphest",
        "Funding": "https://www.softwareheritage.org/donate",
        "Source": "https://forge.softwareheritage.org/source/swh-core",
        "Documentation": "https://docs.softwareheritage.org/devel/swh-core/",
    },
)
