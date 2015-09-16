import os
import sys
import re
import subprocess

CLI_UTILS_ENCODING = sys.stdout.encoding

def parse_maj_min(s):
    m = re.match(r'(\d*):(\d*)', s)
    assert m
    return int(m.group(1)), int(m.group(2))

def is_partition_dirent(device_name, entry):
    if not entry.startswith(device_name):
        return False
    return os.path.exists(os.path.join('/sys', 'block', device, entry, 'start'))

def read_sysfs(path, filename):
    with open(os.path.join(path, filename), 'r') as f:
        data = f.read()
    try:
        return int(data)
    except ValueError:
        return data

class Device:
    def __init__(self, name):
        self.name = name
        self.partitions = None
        self.holders = None
        self.size = None
        self.major = None
        self.minor = None
        self.lsblk = None
        self.by = {}

    @staticmethod
    def from_sysfs(device_name):
        dev = Device(device_name)
        path = os.path.join('/sys', 'block', device_name)
        partition_names = []
        for entry in os.listdir(path):
            if is_partition_dirent(device_name, entry):
                partition_names.append(entry)
            elif entry == 'holders':
                dev.holders = os.listdir(os.path.join('/sys', 'block', device_name, 'holders'))
            elif entry == 'dev':
                dev.major, dev.minor = parse_maj_min(read_sysfs(path, entry))
            elif entry == 'size':
                dev.size = int(read_sysfs(path, entry))

        dev.partitions = [Partition.from_sysfs(part_name, dev)
                          for part_name in partition_names]
        return dev

class Partition:
    def __init__(self, name, device):
        self.name = name
        self.device = device
        self.holders = None
        self.lsblk = None
        self.by = {}

    @staticmethod
    def from_sysfs(name, device):
        part = Partition(name, device)
        path = os.path.join('/sys', 'block', device.name, part.name)
        entries = os.listdir(path)
        for entry in entries:
            if entry == 'holders':
                part.holders = os.listdir(os.path.join('/sys', 'block', device, part, 'holders'))
            elif entry == 'dev':
                part.major, part.minor = parse_maj_min(read_sysfs(path, entry))
        return part

class Host:
    def __init__(self):
        self.devices = None
        self.partitions = None
        self.missing_from_lsblk = None

    def entity(self, name):
        if name in self.devices:
            return self.devices[name]
        elif name in self.partitions:
            return self.partitions[name]
        else:
            raise KeyError()

    @staticmethod
    def go(args):
        host = Host.from_sysfs(args)
        results = Host.from_lsblk(args)
        host.missing_from_lsblk = (set(self.devices.keys())
                                   + set(self.partitions.keys())
                                   - set(result[PRIMARY_KEY] for result in results))
        host._punch_up_lsblk(results)
        host._punch_up_dev_disk()
        return host

    @staticmethod
    def from_sysfs(args):
        host = Host()
        host.devices = {}
        host.partitions = {}

        def device_names():
            for entry in os.listdir(os.path.join('/sys', 'block')):
                if args.all_devices or not re.fullmatch(r'(?:ram\d+|loop\d+)', entry):
                    yield entry

        for dev_name in device_names():
            dev = Device.from_sysfs(dev_name)
            host.devices[dev.name] = dev

            for part in dev.partitions:
                host.partitions[part.name] = part

        return host

    @staticmethod
    def from_lsblk(args):
        cmd = ['lsblk']
        if args.all_devices:
            cmd.append('--all')
        cmd.extend(['-P', '-O'])
        out = subprocess.check_output(cmd)
        results = []
        for l in out.decode(CLI_UTILS_ENCODING).splitlines():
            a = re.findall(r'(.*?)="(.*?)" ?', l)
            d = {k: v for k, v in a}
            results.append(d)
        return results

    def _punch_up_lsblk(self, rows):
        PRIMARY_KEY = 'NAME'
        for row in rows:
            name = row[PRIMARY_KEY]

            try:
                entity = self.entity(name)
            except KeyError:
                raise RuntimeError("device '{}' in lsblk results not in /sys/block/*/*".format(name))

            entity.lsblk = row
            assert '{}:{}'.format(entity.major, entity.minor) == entity.lsblk['MAJ:MIN']
            assert entity.name == entity.lsblk[PRIMARY_KEY]

    def _punch_up_dev_disk(self):
        for kind in os.listdir(os.path.join('/dev', 'disk')):
            path = os.path.join('/dev', 'disk', kind)
            for entry in os.listdir(path):
                entity_name = os.path.basename(os.readlink(os.path.join(path, entry)))

                try:
                    entity = self.entity(entity_name)
                except KeyError:
                    raise RuntimeError("device '{}' (linked from /dev/disk/{}/{}) "
                                       "not in /sys/block/*/*".format(name, kind, entry))

                if kind == 'by-partuuid':
                    assert entity.lsblk['PARTUUID'] == entry
                elif kind == 'by-uuid':
                    assert entity.lsblk['UUID'] == entry
                else:
                    entity.by[kind] = entry

##### xxx

def parse_zpool_status(status):
    config = False
    rv = {}
    for l in status.decode(CLI_UTILS_ENCODING).splitlines():
        if config:
            if l == '':
                config = False
                continue
            #
            l = l.lstrip('\t')
            pos = len(l) - len(l.lstrip(' '))
            assert pos % 2 == 0
            pos //= 2
            part = l.lstrip(' ').split()[0]
            if part == 'spares' or (len(path) > 1 and path[1] == 'spares'):
                pos += 1
            path = path[0:pos]
            path.append(None)
            path[pos] = part

            #
            if len(path) == 3:
                if (path[2] in rv and
                    rv[path[2]].endswith('spares')):
                    rv[path[2]] = '*.spares'
                else:
                    assert path[2] not in rv
                    rv[path[2]] = path[0] + '.' + path[1]
        else:
            if re.match(r'\s*NAME\s*STATE\s*READ\s*WRITE\s*CKSUM', l):
                config = True
                path = []
            continue
    return rv

def walk_dev_zvol():
    if not os.path.exists('/dev/zvol'):
        return
    for pool in os.listdir('/dev/zvol'):
        poolpath = os.path.join('/dev/zvol', pool)
        for vol in os.listdir(poolpath):
            volpath = os.path.join(poolpath, vol)
            devpath = os.path.abspath(os.path.join(os.path.dirname(volpath),
                                                   os.readlink(volpath)))
            assert devpath.startswith('/dev/')
            name = devpath[5:]
            yield name, '{}/{}'.format(pool, vol)


def get_data(args):

    # punch up with zpool status, if we can get it without prompting for a password
    try:
        zpool_status = subprocess.check_output(['sudo', '-n', 'zpool', 'status'], stderr=subprocess.STDOUT)
        zpaths = parse_zpool_status(zpool_status)
        if all(v.endswith('-0') for v in zpaths.values()):
            zpaths = {k: v[0:-2] for k, v in zpaths.items()}
        for dev in devices.values():
            vdev = dev.get('by-vdev')
            idd = dev.get('by-id')
            if vdev and vdev in zpaths:
                dev['zpath'] = zpaths[vdev]
            elif idd and idd in zpaths:
                dev['zpath'] = zpaths[idd]
    except subprocess.CalledProcessError as ex:
        if ex.output == 'sudo: a password is required\n' and ex.returncode == 1:
            print("WARNING: couldn't get zpool status non-interactively; consider adding this to sudoers:\n")
            print("    {} ALL=NOPASSWD: /sbin/zpool status\n".format(os.environ['USER']))
        else:
            logging.exception(ex)
            print()

    return devices, partitions, missing_from_lsblk
