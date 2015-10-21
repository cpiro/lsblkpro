from setuptools import setup, find_packages

setup(
    name = 'lsblkpro',
    version = '0',
    packages = ['lsblkpro'],
    install_requires = [
        'future>=0.15.2',
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
    keywords = ['lsblk', 'zfs', 'zpool', 'console'],
    url = "https://github.com/cpiro/lsblkpro",
    bugtrack_url = "https://github.com/cpiro/lsblkpro/issues",
)
