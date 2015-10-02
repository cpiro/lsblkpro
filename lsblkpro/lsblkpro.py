#!/usr/bin/env python3.4

# TODO attach time
# TODO package desc should include /sys/block
# TODO metric and binary sizes
# TODO s.m.a.r.t. support (temp?)
# TODO optionally trunc [...] long cells when most of column is short
# TODO -A restarts with `less -S`
# TODO show and highlight misalignment, smart, other warning signs
# TODO deal with resilvering
# TODO sort by everything, esp vdev

# 3to2
# xxx special grid view
# xxx strip -0 in zpaths per pool
# xxx iostat column?

import os
import sys
import re
import argparse
import collections
import itertools

import struct
import fcntl
import termios

import pprint
pp = pprint.pprint

from . import data

def pad_maj_min(text):
    v = ' ' * (3-text.index(':')) + text
    return v + ' ' * (7-len(v))

def terminal_size():
    h, w, hp, wp = struct.unpack('HHHH',
                       fcntl.ioctl(0, termios.TIOCGWINSZ,
                           struct.pack('HHHH', 0, 0, 0, 0)))
    return h, w

def width_limit(args):
    if args.all_columns:
        return None
    else:
        try:
            _, width = terminal_size()
            return width - 1
        # xxx if output is not a tty then be sure not to limit width
        except Exception:
            return None

FORMAT_OPTIONS = {
    'display_name': '<',
    'location': '<',
    'TRAN': '>',
    'HCTL': '<',
    'id': '<',
    'path': '<',
    'name': '<',
    'NAME': '<',
    'KNAME': '<',
    'zpool': '>',
    'vdev': '>',
    'MOUNTPOINT': '<',
}

ALWAYS_INTERESTING = {
    'SIZE'
}

IMPORTANCE_ORDER = {key: ii for ii, key in enumerate([
    'display_name',
    'location',

    'name',
    'NAME',
    'KNAME',
    'vdev',
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
    'id',
    'partlabel',
    'path',
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
])}

DISPLAY_ORDER = {key: ii for ii, key in enumerate([
    'display_name',
    'vdev',
    'location',
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
])}

DUPLICATES = (
    ('KNAME', 'NAME'),
    ('partuuid', 'PARTUUID'),
    ('uuid', 'UUID'),
    ('partlabel', 'PARTLABEL'),
)

########

class Column:
    def __init__(self, key):
        self.key = key
        self.width = len(self.header_cell)
        self.unique = True
        self.unique_value = None

    def update(self, row):
        cell = self.cell_for(row)
        if self.unique:
            if self.unique_value is None:
                self.unique_value = cell
            else:
                self.unique = (self.unique_value == cell)

        self.width = max(self.width, len(cell))

    def __repr__(self):
        return "<{}: w={}>".format(self.key, self.width)

    @property
    def header_cell(self):
        return self.key

    def cell_for(self, row): # xxx None |-> ''
        if self.key == 'FSTYPE' and not row.show_fstype:
            return ''

        lookups = (
            getattr(row, self.key, None),
            getattr(row.ent, self.key, None),
            row.ent.lsblk.get(self.key, None),
            row.ent.by.get(self.key, None),
        )
        matches = tuple(filter(None, lookups))
        assert len(matches) <= 1, "table key '{}' not unique for {}".format(self.key, row)
        if matches:
            return str(matches[0])
        else:
            return ''

    def formatted_cell_for(self, row): # row=None means header
        if row is None:
            text = self.header_cell
        else:
            text = self.cell_for(row)

        if self.key == 'MAJ:MIN':
            text = pad_maj_min(text)

        if '•' in text:
            a, b = text.split('•')
            sep = ' ' * (self.width - len(a) - len(b))
            text = a + sep + b

        fmt = FORMAT_OPTIONS.get(self.key)
        if fmt in ('>', '<'):
            fmt += str(self.width)
        elif fmt is None:
            fmt = '>' + str(self.width)

        return "{0:{fmt}}".format(text, fmt=fmt)

    class DefaultDict(collections.defaultdict):
        def __missing__(self, v):
            col = Column(v)
            self[v] = col
            return col

class Table:
    def __init__(self, rows, width_limit=None, include=[], exclude=[]):
        self.rows = list(rows)
        self.cols = Column.DefaultDict()

        for row in self.rows:
            for key in row:
                col = self.cols[key]
                col.update(row)

        duplicates = {(a, b) for a, b in DUPLICATES if self.are_duplicates(a, b)}
        unique = {key for key, col in self.cols.items()
                  if col.unique and key not in ALWAYS_INTERESTING
                 } if len(self.rows) > 1 else {}
        omit = set(a for a, b in duplicates) | unique | set(exclude) - set(include)

        def importance_order(key):
            if key in include:
                return -9999
            else:
                return IMPORTANCE_ORDER.get(key, 9999)

        importance = sorted((col for col in self.cols if col not in omit),
                            key=importance_order)

        remaining_width = width_limit
        # pack columns into allotted width (greedy)
        columns = []
        overflow = []
        for key in importance:
            col = self.cols[key]
            if width_limit is None or col.width <= remaining_width:
                columns.append(key)
                remaining_width -= col.width + 1
            else:
                overflow.append(key)

        self.duplicates = duplicates
        self.unique = unique
        self.overflow = overflow
        self.columns = sorted(columns, key=lambda k: DISPLAY_ORDER.get(k, 9999))

    def are_duplicates(self, a, b):
        try:
            col_a = self.cols[a]
            col_b = self.cols[b]
        except KeyError:
            return False

        return all(col_a.cell_for(row) == col_b.cell_for(row)
                   for row in self.rows)

    def print_(self):
        if self.duplicates or self.unique:
            lwidth = max(max(len(a) for a, _ in self.duplicates),
                         max(len(k) for k in self.unique))
            print("Every device has these fields:")
            for a, b in self.duplicates:
                print("  {0:{lwidth}} = <{1}>".format(a, b, lwidth=lwidth))
            for k in self.unique:
                print("  {0:{lwidth}} = {1}".format(k, self.cols[k].unique_value, lwidth=lwidth))
            print()

        if self.overflow:
            print("Overflowing labels:\n  {}\n".format(', '.join(sorted(self.overflow))))


        line = ' '.join(self.cols[col].formatted_cell_for(None) for col in self.columns)
        print('\033[1m' + line + '\033[0m')

        for row in self.rows:
            line = ' '.join(self.cols[col].formatted_cell_for(row) for col in self.columns)
            print(row.color + line)

class Row:
    def __init__(self, ent):
        self.ent = ent
        self.display_name = None
        self.synthesized = ['NAME', 'zpath']

        self.color = '' # xxx

    def __iter__(self):
        chain = itertools.chain(
            ('display_name', 'location', 'zpath'),
            self.ent.lsblk.keys(),
            self.ent.by.keys(),
        )
        yield from (key for key in chain if key not in self.synthesized)

    def __getitem__(self, label):
        return self.ent._sort_value(label)

    def __contains__(self, label):
        try:
            self.ent._sort_value(label)
            return True
        except KeyError:
            return False

    @property
    def show_fstype(self):
        return self.ent.lsblk['FSTYPE'] not in ('', 'linux_raid_member', 'zfs_member')

    #xxx
    #def show_BY-vdev-if-partition(self): False


    def _display_name(self, *, last):
        lsblk, by = self.ent.lsblk, self.ent.by

        name = lsblk['KNAME']
        if lsblk['NAME'] != name:
            name += '={}'.format(lsblk['NAME'])
        vdev = ('•{}'.format(by['vdev']) if (by.get('vdev') and
                                             isinstance(self.ent, data.Partition))
                                         else '')
        typ = ('•({})'.format(lsblk['TYPE']) if not (lsblk.get('TYPE')
                                                           in (None, 'disk', 'part', 'md')
                                                         or (lsblk.get('TYPE') == 'loop'
                                                             and self.ent.name.startswith('loop')))
                                           else '')

        if isinstance(self.ent, data.Partition):
            return (BOX_END if last else BOX_MID) + name + vdev + typ
        else:
            return name + vdev + typ

    @property
    def location(self):
        mnt = self.ent.lsblk.get('MOUNTPOINT')
        holders = '[{}]'.format(', '.join(self.ent.holder_names)) if self.ent.holder_names else ''
        assert len(list(filter(None, (self.ent.zpath, mnt)))) <= 1
        return ' '.join(filter(None, (self.ent.zpath, mnt, holders)))

    @staticmethod
    def rows_for(host, row_ents):
        for ii, ent in enumerate(row_ents):
            try:
                last = isinstance(row_ents[ii+1], data.Device)
            except IndexError:
                last = True
            row = Row(ent)
            row.display_name = row._display_name(last=last)
            yield row

# xxx
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
        row.color = color_table[row[field]]

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

    # data
    if args.load_data:
        import pickle
        with open('data', 'rb') as f:
            host = pickle.load(f)
    else:
        host = data.Host.go(args)

    if args.store_data:
        assert not args.load_data
        import pickle
        with open('data', 'wb') as f:
            pickle.dump(host, f)
        sys.exit(0)

    # compute rows (each device followed by its partitions)
    def display_order_for():
        for device in host.devices_sorted(args):
            yield device
            if args.only_devices:
                continue
            if (device.by.get('vdev') or device.zpath) and not args.all_devices:
                continue
            for part in device.partitions:
                assert part.lsblk['PKNAME'] == device.name
                yield part

    def apply_filters(row_ents):
        filter_log = []
        # xxx allow relative by parsing sizes
        for f in args.filters:
            if '=~' in f:
                lhs, rhs = f.split('=~', 1)
                filter_log.append("{} matches regexp /{}/".format(lhs, rhs))
                row_ents = filter(lambda ent: re.match(rhs, ent._sort_value(lhs), row_ents))
            elif '=' in f:
                lhs, rhs = f.split('=', 1)
                filter_log.append("{} = {}".format(lhs, rhs))
                row_ents = filter(lambda ent: ent._sort_value(lhs) == rhs, row_ents)
            elif '!=' in f:
                lhs, rhs = f.split('!=', 1)
                filter_log.append("{} != {}".format(lhs, rhs))
                row_ents = filter(lambda ent: ent._sort_value(lhs) != rhs, row_ents)
            else:
                filter_log.append("{} is set".format(f))
                row_ents = filter(lambda ent: ent._sort_value(f), row_ents)

        return list(row_ents), filter_log

    row_ents, filter_log = apply_filters(display_order_for())

    # munge
    # xxx munge_highlights(rows, args.highlight)
    #view = View(rows)
    #view._figure_out_labels(args)

    if host.missing_from_lsblk:
        print("Present in sysfs but not in `lsblk`:\n  {}\n".format(', '.join(host.missing_from_lsblk)))

    rows = Row.rows_for(host, row_ents)
    table = Table(rows, width_limit=width_limit(args), include=args.include, exclude=args.exclude)
    table.print_()

    sys.exit(0)

    # pre-print
    if filter_log:
        print("Showing only entries where:")
        for f in filter_log:
            print("  {}".format(f))
        print()

    view.print_table()
