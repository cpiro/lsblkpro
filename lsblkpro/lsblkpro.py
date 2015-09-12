#!/usr/bin/env python3.4

# TODO attach time
# xxx package desc should include /sys/block
# 3to2

# xxx metric and binary sizes
# xxx s.m.a.r.t. support (temp?)
# xxx optionally trunc [...] long cells when most of column is short

import os
import sys
import re
import argparse
import collections
import operator
import itertools
import string

import struct
import fcntl
import termios

import pprint
pp = pprint.pprint

from . import data

always_interesting = set('SIZE')

importance = [
    'displayname',
    'location',

    'name',
    'NAME',
    'KNAME',
    'by-vdev',
    'zpath',
    'MOUNTPOINT',
    'size',
    'SIZE',
    'FSTYPE',
    'HCTL',
    'MAJ:MIN',
    'TRAN',
    'RA',
    'RQ-SIZE',
    'OWNER',
    'GROUP',
    'MODE',
    'MODEL',
    'RO',
    'RM',
    'by-id',
    'by-partlabel',
    'by-path',
    'UUID',
    'ALIGNMENT',
    'MIN-IO',
    'OPT-IO',
    'TYPE',
    'ROTA',
    'PHY-SEC',
    'LOG-SEC',
    'WWN',
    'PARTUUID',
    'PARTTYPE',
    'PARTLABEL',
    'SERIAL',
    'DISC-ALN', 'DISC-GRAN', 'DISC-MAX', 'DISC-ZERO',
    'STATE',
    'PARTFLAGS',
    'LABEL',
    'SCHED',
    'VENDOR',
    'RAND',
    'REV',
    'WSAME',
]

SORT_ORDER = {key: value for value, key in enumerate([
    'displayname',
    'by-vdev',
    'location',

    'name',
    'NAME',
    'KNAME',
    'zpath',
    'MOUNTPOINT',
    'FSTYPE',
    'SIZE',
    'TRAN',
    'HCTL',
    'MAJ:MIN',
    'OWNER',
    'GROUP',
    'MODE',
    'TYPE',
    'ROTA',
    # 'ALIGNMENT',
    # 'MIN-IO',
    # 'OPT-IO',
    # 'PHY-SEC',
    # 'LOG-SEC',
    # 'RO',
    # 'RM',
    # 'DISC-ALN', 'DISC-GRAN', 'DISC-MAX', 'DISC-ZERO',
    # 'MODEL',
    # 'STATE',
    # 'LABEL',
    # 'FSTYPE',
    # 'VENDOR',
    # 'UUID',
    # 'WWN',
    # 'SERIAL',
    ])}

DUPLICATES = (
    ('KNAME', 'name'),
    ('by-partuuid', 'PARTUUID'),
    ('by-uuid', 'UUID'),
    ('by-partlabel', 'PARTLABEL'),
)

def terminal_size():
    h, w, hp, wp = struct.unpack('HHHH',
                       fcntl.ioctl(0, termios.TIOCGWINSZ,
                           struct.pack('HHHH', 0, 0, 0, 0)))
    return h, w

def label_to_str(_, l, width=None):
    if l == 'displayname':
        return 'DEVICE'
    elif l == 'location':
        return ''
    elif l.startswith('by-'):
        return l[3:]
    else:
        return l

def value_to_str(row, label):
    if label == 'MAJ:MIN':  # align the colons
        v = ' ' * (3-row[label].index(':')) + row[label]
        return v + ' ' * (7-len(v))
    elif label in row:
        return str(row[label])
    else:
        return ''

def value_to_str_bullets(r, l, width):
    s = value_to_str(r, l)
    if '•' in s:
        a, b = s.split('•')
        sep = ' ' * (width - len(a) - len(b))
        return a + sep + b
    else:
        return s

def width_for_column(label, rows):
    return max(
        len(label_to_str(label, label)),
        max(len(value_to_str(row, label)) for row in rows)
    )

class DeviceCollection:
    # xxx lift non-lsblk data (holders, etc.) all the way out of data module
    def __init__(self, devices, partitions, missing_from_lsblk, zvols):
        self._devices = devices
        self._partitions = partitions
        self._missing_from_lsblk = missing_from_lsblk
        self._zvols = zvols

        self.devices = sorted((Device(devname, collection=self)
                               for devname in self._devices.keys()),
                              key=operator.attrgetter('sort_name'))
        self.partitions = [Partition(partname, collection=self)
                           for partname in self._partitions.keys()]
        self.missing_from_lsblk = sorted((Device(missname, collection=self)
                                          for missname in self._missing_from_lsblk),
                                         key=operator.attrgetter('sort_name'))

        self._devices_by_name = {device.name: device for device in self.devices}
        self._partitions_by_name = {part.name: part for part in self.partitions}

    def devices_specified_order(self, args):
        def specified_order(device):
            _dev = device.data
            lex = [_dev.get(key, '') for key in args.sorts]
            lex.append(device.name_parts)
            return lex

        names = sorted(self._devices_by_name.values(), key=specified_order)
        return (self._devices_by_name[name] for name in names)

    def devices_smart_order(self):
        todo = {device.name for device in self.devices}

        held_by = collections.defaultdict(list)
        for device in sorted(self.devices, key=operator.attrgetter('sort_name')):
            for holder in device.holders:
                held_by[holder.name].append(device.name)
                todo.discard(holder.name)
                todo.discard(device.name) # only remove if this device has holders

        holder_groups = [(tuple(group), holder) for holder, group in held_by.items()]
        holder_groups.extend(((devname,), ()) for devname in todo)

        for group, holder in sorted(holder_groups, key=lambda elt: elt[0][0]):
            yield from (self._devices_by_name[name] for name in group)
            if holder:
                yield self._devices_by_name[holder]

class Device:
    def __init__(self, name, *, collection):
        self.name = name
        self.collection = collection

    @property
    def data(self):
        return self.collection._devices[self.name]

    @property
    def sort_name(self):
        tup = list(self.name_parts)
        if isinstance(tup[1], str):
            tup[1] = Device.device_letters_to_int(tup[1])
        return tup

    @property
    def partitions(self):
        for partname in self.data['partitions']:
            _part = self.collection._partitions[partname]
            assert _part['PKNAME'] == self.name
            assert _part['name'] == partname
            yield self.collection._partitions_by_name[partname]

    @property
    def holders(self):
        # holders of this device ...
        yield from (self.collection._devices_by_name[name]
                    for name in self.data.get('holders', ()))

        # ... and holders of its partitions
        for part in self.partitions:
            yield from part.holders

    @property
    def name_parts(self):
        def to_int_maybe(p):
            try:
                return int(p)
            except ValueError:
                return p

        tup = tuple(to_int_maybe(part) for part in re.findall(r'(?:^[a-z]{2}-?|[a-z]+|\d+)', self.name))
        assert ''.join(str(part) for part in tup) == self.name
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
    def __init__(self, name, *, collection):
        self.name = name
        self.collection = collection

    @property
    def data(self):
        return self.collection._partitions[self.name]

    @property
    def holders(self):
        return (self.collection._devices_by_name[name]
                for name in self.data.get('holders', []))

def display_order_for(devc, args):
    rows = []
    if args.sorts:
        devices = devc.devices_specified_order(args)
    else:
        devices = devc.devices_smart_order()

    for dev in devices:
        _dev = dev.data
        rows.append(_dev)
        if args.only_devices:
            continue
        if (_dev.get('vdev') or _dev.get('zpath')) and not args.all_devices:
            continue
        for part in dev.partitions:
            _part = part.data
            assert _part['PKNAME'] == dev.name
            rows.append(_part)
    return rows

def apply_filters(rows, args):
    filter_log = []
    # xxx allow relative by parsing sizes
    for f in args.filters:
        if '=~' in f:
            lhs, rhs = f.split('=~', 1)
            filter_log.append("{} matches regexp /{}/".format(lhs, rhs))
            rows = [row for row in rows if re.match(rhs, row[lhs])]
        elif '=' in f:
            lhs, rhs = f.split('=', 1)
            filter_log.append("{} = {}".format(lhs, rhs))
            rows = [row for row in rows if row[lhs] == rhs]
        elif '!=' in f:
            lhs, rhs = f.split('!=', 1)
            filter_log.append("{} != {}".format(lhs, rhs))
            rows = [row for row in rows if row[lhs] != rhs]
        else:
            filter_log.append("{} is set".format(f))
            rows = [row for row in rows if row.get(f)]

    return rows, filter_log

def figure_out_labels(rows, args):
    omit = {
        'NAME', 'PKNAME', 'name', 'zpath', 'MOUNTPOINT', 'TYPE', 'by-vdev', 'holders', 'partitions', # used by munge
        'major', 'minor',  'size', # xxx
        'MODEL', # boring
    }

    omit.update(args.exclude)
    omit -= set(args.include)

    every_device_has = []
    for candidate, reference in DUPLICATES:
        if all(row.get(candidate, '') == row.get(reference, '') for row in rows):
            every_device_has.append((candidate, '<{}>'.format(reference)))
            omit.add(candidate)

    width_label_pairs = []
    overflow = []
    running_width = 0
    if args.all_columns:
        width_limit = None
    else:
        try:
            _, width_limit = terminal_size()
            width_limit -= 1
            # xxx if output is not a tty then be sure not to limit width
        except Exception:
            width_limit = None

    for label in args.include:
        importance.remove(label)
        always_interesting.add(label)
    importance[2:1+len(args.include)] = args.include

    for label in importance:
        if label in omit:
            continue

        values_in_this_column = set(value_to_str(r, label) for r in rows)
        if (label in always_interesting or
            not (len(rows) == 1 or len(values_in_this_column) == 1)):
            width = width_for_column(label, rows)

            if width_limit is not None and running_width + width > width_limit:
                overflow.append(label)
            else:
                running_width += width + 1
                width_label_pairs.append((width, label))
        else:
            val = values_in_this_column.pop()
            if val:
                every_device_has.append((label, val))

    return width_label_pairs, every_device_has, omit, overflow

def munge(rows, devices, partitions, zvols):
    def display_name_for(row, *, last):
        name = row['KNAME']
        if row['NAME'] != name:
            name += '={}'.format(row['NAME'])
        vdev = ('•{}'.format(row['by-vdev']) if (row.get('by-vdev') and
                                                 row['name'] not in partitions)
                                             else '')
        typ = ('•({})'.format(row['TYPE']) if not (row.get('TYPE') in (None, 'disk', 'part', 'md')
                                                   or (row.get('TYPE') == 'loop'
                                                       and row['name'].startswith('loop')))
                                           else '')

        if row['name'] in partitions:
            return (BOX_END if last else BOX_MID) + name + vdev + typ
        else:
            return name + vdev + typ

    def location_for(row):
        zpath = row.get('zpath', '')
        mnt = row.get('MOUNTPOINT', '')
        zvol = zvols.get(row['name'], '')
        holders = '[{}]'.format(', '.join(row['holders'])) if row.get('holders') else ''
        assert sum(1 for x in (zpath, mnt, zvol) if x) <= 1
        return ' '.join(x for x in (zpath, mnt, zvol, holders) if x)

    for ii, row in enumerate(rows):
        try:
            last = rows[ii+1]['name'] in devices
        except IndexError:
            last = True
        row['displayname'] = display_name_for(row, last=last)
        if row['name'] in partitions:
            row['by-vdev'] = ''
        if row['FSTYPE'] in ('', 'linux_raid_member', 'zfs_member'):
            row['FSTYPE'] = ''
        row['location'] = location_for(row)

def munge_highlights(rows, field):
    if field is None:
        return
    # i'm colorblind gimme a break
    color_list = ['0', '31', '32', '34', '41', '42', '44', '45', '30;46', '30;47']
    color_table = {}  # value => color
    color = None
    for row in rows:
        if row[field] not in color_table:
            try:
                color = color_list.pop(0)
                color_table[row[field]] = "\033[{}m".format(color)
            except IndexError:
                print("fatal: not enough colors to highlight by '{}'".format(field))
                sys.exit(0)
        row['$color'] = color_table[row[field]]

def print_table(width_label_pairs, rows):
    format_options = {
        'displayname': '<',
        'location': '<',
        'TRAN': '>',
        'HCTL': '<',
        'by-id': '<',
        'by-path': '<',
        'name': '<',
        'NAME': '<',
        'KNAME': '<',
        'zpool': '>',
        'by-vdev': '>',
        'MOUNTPOINT': '<',
        }

    def print_row(r, xform):
        def get_format(l, w):
            fmt = format_options.get(l)
            if fmt in ('>', '<'):
                fmt += str(w)
            elif fmt is None:
                fmt = '>' + str(w)
            return fmt

        cells = ("{0:{fmt}}".format(xform(r, l, width=w), fmt=get_format(l, w))
                 for w, l in width_label_pairs)
        line = ' '.join(cells)

        color = r.get('$color')
        if color is not None:
            line = color + line + '\033[0m'

        print(line)

    print_row({l: l for w, l in width_label_pairs}, label_to_str)
    for r in rows:
        print_row(r, value_to_str_bullets)

def main():
    # argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("-d", "--only-devices", action='store_true',
                        help="show only devices (not partitions)")
    parser.add_argument("-i", "--include", action='append', dest='include', default=[],
                        help="include these fields in the output")
    parser.add_argument("-e", "--exclude", action='append', dest='exclude', default=[],
                        help="exclude these fields from the output")
    # xxx expose field names that aren't labels
    # xxx allow 'size' but not 'SIZE' (which is lsblk's string)
    parser.add_argument("-x", "--sort", action='append', dest='sorts', default=[],
                        help="sort devices by field(s); implies -i")
    parser.add_argument("-w", "--where", action='append', dest='filters', default=[],
                        help="filters e.g. NAME=sdc, vdev=a4")
    parser.add_argument("-g", "--highlight",
                        help="highlight entries by a field")
    parser.add_argument("-a", "--all-devices", action='store_true',
                        help="include ram* and loop* devices, and include partitions of zpool drives")
    parser.add_argument("-A", "--all-columns", action='store_true',
                        help="include all columns, appropriate to pipe to `less -S`")
    parser.add_argument("--ascii", action='store_true',
                        help="use ASCII characters for tree formatting")
    parser.add_argument("--store-data", action='store_true',
                        help="")
    parser.add_argument("--load-data", action='store_true',
                        help="")

    args = parser.parse_args()

    args.include.extend(args.sorts)

    if not (sys.platform.startswith('linux') or args.load_data):
        print("{}: fatal error: Linux is required".format(os.path.basename(sys.argv[0])))
        sys.exit(1)

    global BOX_MID, BOX_END
    if sys.stdout.encoding == 'UTF-8' and not args.ascii:
        BOX_MID, BOX_END = ' ├─ ', ' └─ '
    else:
        BOX_MID, BOX_END = ' |- ', ' `- '

    if args.load_data:
        import pickle
        with open('data', 'rb') as f:
            pickled = pickle.load(f)
            devices, partitions, missing_from_lsblk, zvols = (
                pickled[k] for k in ('devices', 'partitions', 'missing_from_lsblk', 'zvols'))
    else:
        devices, partitions, missing_from_lsblk = data.get_data(args)
        zvols = {name: zvol for name, zvol in data.walk_dev_zvol()}

    if args.store_data:
        assert not args.load_data
        import pickle
        with open('data', 'wb') as f:
            pickle.dump({
                'devices': devices,
                'partitions': partitions,
                'missing_from_lsblk': missing_from_lsblk,
                'zvols': zvols,
            }, f)
        sys.exit(0)

    # compute rows (each device followed by its partitions)
    devc = DeviceCollection(devices, partitions, missing_from_lsblk, zvols)
    rows = display_order_for(devc, args)
    rows, filter_log = apply_filters(rows, args)

    # munge
    munge(rows, devices, partitions, zvols)
    munge_highlights(rows, args.highlight)

    # figure out labels
    width_label_pairs, every_device_has, omit, overflow = figure_out_labels(rows, args)

    # pre-print
    if filter_log:
        print("Showing only entries where:")
        for f in filter_log:
            print("  {}".format(f))
        print()

    if every_device_has:
        print("Every device has these fields:")
        lwidth = max(len(l) for l, _ in every_device_has)
        for l, v in every_device_has:
            print("  {0:{lwidth}} = {1}".format(l, v, lwidth=lwidth))
        print()

    # labels in `rows` not in importance or omit
    all_labels = set()
    for row in rows:  # victoresque
        all_labels |= row.keys()

    missing_labels = all_labels - set(importance) - omit
    if missing_labels:
        print("Missing labels:\n  {}\n".format(', '.join(sorted(missing_labels))))

    if overflow:
        print("Overflowing labels:\n  {}\n".format(', '.join(sorted(overflow))))

    if missing_from_lsblk:
        print("Present in sysfs but not in `lsblk`:\n  {}\n".format(', '.join(sorted(missing_from_lsblk, key=dev_name_split))))

    # print
    def column_order(elt):
        w, l = elt
        if l in SORT_ORDER:
            return SORT_ORDER[l]
        else:
            return w + 1000 # shorter ones first

    width_label_pairs.sort(key=column_order)
    print_table(width_label_pairs, rows)
