from __future__ import (absolute_import, division, print_function, unicode_literals)
from builtins import *

import os
import sys
import re
import string
import operator
import subprocess
import collections

CLI_UTILS_ENCODING = sys.stdout.encoding
PRIMARY_KEY = 'NAME'

class Entity(object):
    def __init__(self, name):
        self.name = name
        self.lsblk = {}
        self.by = {}
        self.zpath = None
        self.holder_names = None

class Device(Entity):
    def __init__(self, name):
        super().__init__(name)
        self.partitions = None
        self.major = None
        self.minor = None

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

        dev.partitions = [Partition.from_sysfs(part_name, dev)
                          for part_name in partition_names]
        return dev

    @property
    def name_parts(self):
        return Device.name_parts_for(self.name)

    @property
    def _sortable_smart(self):
        return Device._sortable_smart_for(self.name)

    @staticmethod
    def name_parts_for(name):
        #XXX
        #if name.startswith('dm-'):
        #    return name

        def to_int_maybe(p):
            try:
                return int(p)
            except ValueError:
                return p

        # https://www.kernel.org/doc/Documentation/devices.txt
        tup = tuple(to_int_maybe(part) for part in re.findall(r'(?:^(?:dm-|zd|ram|fd|hd|loop|sd|n?st|md|scd|n?tpqic|xd|sonycd|gscd|optcd|sjcd|c?double|hitcd|sg|mfm|hd|mcd|cdu535|sbpcd|qft|nqft|zqft|nzqft|rawqft|nrawqft|ad|aztcd|cm205cd|r?rom|r?flash|cm206cd|slram|n?ht|z2ram|nb|ft|pd|pcd|pf|r?pda|sch|mtdr?|ppdd|nft|dasd|n?pt|inft|pg|ubd|jsfd|nnpfs|ub|xvd|n?osst|rfd|ssfdc|blockrom|osd)|^[a-z]-?|[a-z]+|\d+)', name))
        assert ''.join(str(part) for part in tup) == name
        return tup

    @staticmethod
    def _sortable_smart_for(name):
        tup = list(Device.name_parts_for(name))
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

class Partition(Entity):
    def __init__(self, name, device):
        super().__init__(name)
        self.device = device

    @staticmethod
    def from_sysfs(name, device):
        part = Partition(name, device)
        path = os.path.join('/sys', 'block', device.name, part.name)
        entries = os.listdir(path)
        for entry in entries:
            if entry == 'holders':
                part.holder_names = os.listdir(os.path.join('/sys', 'block',
                                                            device.name, part.name, 'holders'))
            elif entry == 'dev':
                part.major, part.minor = parse_maj_min(read_sysfs(path, entry))
        return part

class Host(object):
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

    def devices_smart_order(self):
        todo = set(self.devices.keys())

        held_by = collections.defaultdict(list)
        for device in sorted(self.devices.values(), key=operator.attrgetter('_sortable_smart')):
            for holder_name in device.holder_names:
                held_by[holder_name].append(device.name)
                todo.discard(holder_name)
                todo.discard(device.name) # only remove if this device has holders

        holder_groups = [(tuple(group), holder) for holder, group in held_by.items()]
        holder_groups.extend(((device_name,), ()) for device_name in todo)

        for group, holder_name in sorted(holder_groups, key=lambda elt: Device._sortable_smart_for(elt[0][0])):
            for name in group:
                yield self.devices[name]
            if holder_name:
                yield self.devices[holder_name]

    @staticmethod
    def go(args):
        host = Host.from_sysfs(args)
        results = list(Host.from_lsblk(args))

        host.missing_from_lsblk = sorted(
            (set(host.devices.keys()) | set(host.partitions.keys()))
            - set(result[PRIMARY_KEY] for result in results),
            key=Device._sortable_smart_for)

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
                if args.all_devices or not re.match(r'^(?:ram\d+|loop\d+)$', entry):
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
        cmd.extend(['-P', '-O', '-b'])
        out = subprocess.check_output(cmd)

        for l in out.decode(CLI_UTILS_ENCODING).splitlines():
            yield {k: v for k, v in re.findall(r'(.*?)="(.*?)" ?', l)}

    def _punch_up_lsblk(self, results):
        for entry in results:
            name = entry[PRIMARY_KEY]

            try:
                entity = self.entity(name)
            except KeyError:
                print("warning: device '{}' in lsblk results not in /sys/block/*/*".format(name))
                continue

            entity.lsblk = entry
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
                    if 'UUID' in entity.lsblk:
                        assert entity.lsblk['UUID'] == entry
                    else:
                        print("warning: incomplete lsblk for {}: {}".format(entity.name, entity.lsblk))
                else:
                    assert kind.startswith('by-')
                    entity.by[kind[3:]] = entry

    def _punch_up_zpool_status(self):
        # punch up with zpool status, if we can get it without prompting for a password
        try:
            zpool_status = subprocess.check_output(['sudo', '-n', 'zpool', 'status'],
                                                   stderr=subprocess.STDOUT)
        except subprocess.CalledProcessError as ex:
            if ex.returncode == 1:
                # xxx check if zpool is even installed
                self.zpool_status_result = False
                print("\nWARNING: couldn't get zpool status non-interactively:")
                print(ex.output)
                print("\nconsider adding this to sudoers:\n")
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
    return os.path.exists(os.path.join('/sys', 'block', device_name, entry, 'start'))

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
