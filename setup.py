from setuptools import setup, find_packages

setup(
    name = 'lsblkpro',
    version = '0',
    packages = find_packages(),
    entry_points = {
        'console_scripts': [
            'lsblkpro = lsblkpro.lsblkpro:main',
            'oldlsblkpro = lsblkpro.lsblkpro:old_main',
        ]
    },

    # metadata for upload to PyPI
    author = "Chris Piro",
    author_email = "cpiro@cpiro.com",
    description = "adds ZFS zpool and vdev information, filtering, sorting, and colors to lsblk(8)",
    license = "GPL",
    keywords = "lsblk zfs console xxx",
    url = "xxx",
)
