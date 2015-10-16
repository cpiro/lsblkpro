from __future__ import unicode_literals
from __future__ import print_function
from __future__ import division
from __future__ import absolute_import
from future import standard_library
standard_library.install_aliases()
from setuptools import setup, find_packages

setup(
    name = 'lsblkpro',
    version = '0',
    packages = find_packages(),
    install_requires = [
        'bytesize>=0',
        'pint>=0.6',
    ],
    entry_points = {
        'console_scripts': [
            'lsblkpro = lsblkpro.lsblkpro:main',
        ]
    },

    # metadata for upload to PyPI
    author = "Chris Piro",
    author_email = "cpiro@cpiro.com",
    description = "adds ZFS zpool and vdev information, filtering, and sorting, to lsblk(8)",
    license = "GPL",
    keywords = "lsblk zfs console xxx",
    url = "xxx",
)
