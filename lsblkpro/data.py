import os
import sys
import re
import string
import operator
import subprocess
import collections

CLI_UTILS_ENCODING = sys.stdout.encoding

class Device:
    def __init__(self, name):
        self.name = name
        self.partitions = None
        self.holder_names = None
        self.size = None
        self.major = None
        self.minor = None
        self.lsblk = None
        self.by = {}
        self.zpath = None

    @staticmethod
    def from_sysfs(device_name):
        dev = Device(device_name)
        path = os.path.join('/sys', 'block', device_name)
        partition_names = []
        for entry in os.listdir(path):
            if is_partition_dirent(device_name, entry):
                partition_names.append(entry)
            elif entry == 'holders':
                dev.holder_names = os.listdir(os.path.join('/sys', 'block', device_name, 'holders'))
            elif entry == 'dev':
                dev.major, dev.minor = parse_maj_min(read_sysfs(path, entry))
            elif entry == 'size':
                dev.size = int(read_sysfs(path, entry))

        dev.partitions = [Partition.from_sysfs(part_name, dev)
                          for part_name in partition_names]
        return dev

    @property
    def name_parts(self):
        def to_int_maybe(p):
            try:
                return int(p)
            except ValueError:
                return p

        tup = tuple(to_int_maybe(part) for part
                    in re.findall(r'(?:^[a-z]{2}-?|[a-z]+|\d+)', self.name))
        assert ''.join(str(part) for part in tup) == self.name
        return tup

    def _sort_value(self, key):
        value = self.lsblk.get(key.upper())
        if value:
            return value

        value = self.by.get(key)
        if value:
            return value

        if key.startswith('by-'):
            value = self.by.get(key[3:])
            return value

        raise KeyError("device '{}' has no key '{}'".format(self.name, key))

    def _sortable_specified(self, args):
        return ([self._sort_name(key) for key in args.sorts] +
                self._sortable_smart)

    @property
    def _sortable_smart(self):
        tup = list(self.name_parts)
        if isinstance(tup[1], str):
            tup[1] = Device.device_letters_to_int(tup[1])
        return tup

    @staticmethod
    def device_letters_to_int(letters):
        """spreadsheet column letters to integer index from zero
        e.g. 'a' -> 0, 'z' -> 25, 'aa' -> 26"""
        num = 0
        for l in letters:
            assert l in string.ascii_letters
            num = num * 26 + (ord(l.lower()) - ord('a')) + 1
        return num - 1

class Partition:
    def __init__(self, name, device):
        self.name = name
        self.device = device
        self.holder_names = None
        self.lsblk = None
        self.by = {}

    @staticmethod
    def from_sysfs(name, device):
        part = Partition(name, device)
        path = os.path.join('/sys', 'block', device.name, part.name)
        entries = os.listdir(path)
        for entry in entries:
            if entry == 'holders':
                part.holder_names = os.listdir(os.path.join('/sys', 'block', device, part, 'holders'))
            elif entry == 'dev':
                part.major, part.minor = parse_maj_min(read_sysfs(path, entry))
        return part

class Host:
    def __init__(self):
        self.devices = None
        self.partitions = None
        self.missing_from_lsblk = None

        # True = success, False = need sudoers
        # None = not attempted, Exception = something else
        ##self.zpool_status_result = None

    def entity(self, name):
        if name in self.devices:
            return self.devices[name]
        elif name in self.partitions:
            return self.partitions[name]
        else:
            raise KeyError()

    def devices_sorted(self):
        if args.sorts:
            return sorted(self.devices.values(),
                          key=lambda d: d._sortable_specified(args))

        # smart order
        todo = set(self.devices.keys()}

        held_by = collections.defaultdict(list)
        for device in sorted(self.devices.values(), key=operator.attrgetter('_sortable_smart')):
            for holder_name in device.holder_names:
                held_by[holder_name].append(device.name)
                todo.discard(holder_name)
                todo.discard(device.name) # only remove if this device has holders

        holder_groups = [(tuple(group), holder) for holder, group in held_by.items()]
        holder_groups.extend(((device_name,), ()) for device_name in todo)

        for group, holder_name in sorted(holder_groups, key=lambda elt: elt[0][0]):
            yield from (self.devices[name] for name in group)
            if holder_name:
                yield self.devices[holder_name]

    @staticmethod
    def go(args):
        host = Host.from_sysfs(args)
        results = Host.from_lsblk(args)
        host.missing_from_lsblk = (set(self.devices.keys())
                                   + set(self.partitions.keys())
                                   - set(result[PRIMARY_KEY] for result in results))
        host._punch_up_lsblk(results)
        host._punch_up_dev_disk()
        host._punch_up_zpool_status()

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
                    assert kind.startswith('by-')
                    entity.by[kind[3:]] = entry

    def _punch_up_zpool_status(self):
        # punch up with zpool status, if we can get it without prompting for a password
        try:
            zpool_status = subprocess.check_output(['sudo', '-n', 'zpool', 'status'],
                                                   stderr=subprocess.STDOUT)
        except subprocess.CalledProcessError as ex:
            if ex.output == 'sudo: a password is required\n' and ex.returncode == 1:
                # xxx check if zpool is even installed
                self.zpool_status_result = False
                print("WARNING: couldn't get zpool status non-interactively; consider adding this to sudoers:\n")
                print("    {} ALL=NOPASSWD: /sbin/zpool status\n".format(os.environ['USER']))
            else:
                self.zpool_status_result = ex
            return

        zpaths = parse_zpool_status(zpool_status)
        if all(v.endswith('-0') for v in zpaths.values()):
            # trim off the -0
            zpaths = {k: v[0:-2] for k, v in zpaths.items()}

        for dev in self.devices.values():
            vdev = dev.by.get('vdev')
            idd = dev.by.get('id')
            if vdev and vdev in zpaths:
                dev.zpath = zpaths[vdev]
            elif idd and idd in zpaths:
                dev.zpath = zpaths[idd]

        self.zpool_status_result = True


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
