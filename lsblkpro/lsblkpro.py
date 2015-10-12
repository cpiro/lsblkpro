#!/usr/bin/env python3.4

# TODO prettier errors
# TODO attach time
# TODO package desc should include /sys/block
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
import operator

import struct
import fcntl
import termios

import pprint
pp = pprint.pprint

from . import data

import bytesize

INF = float('inf')

def pad_maj_min(text):
    v = ' ' * (3-text.index(':')) + text
    return v + ' ' * (7-len(v))

def terminal_size():
    h, w, hp, wp = struct.unpack('HHHH',
                       fcntl.ioctl(0, termios.TIOCGWINSZ,
                           struct.pack('HHHH', 0, 0, 0, 0)))
    return h, w

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
    'size': '>',
}

ALWAYS_INTERESTING = {
    'size'
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
    'size',
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

class Table:
    def __init__(self, host, args):
        ents = Table.entity_order_for(host, args)
        self.rows = [Row(ent) for ent in ents]

        if args.sorts:
            self.rows.sort(
                key=lambda row: tuple(row.sort_value(k) for k in args.sorts),
                reverse=args.reverse)
            for row in self.rows:
                row.indent = False

        self.filter_log = []

        filter_pairs = list(Table.filters(args))
        if filter_pairs:
            filters, self.filter_log = zip(*filter_pairs)
            for row in self.rows:
                row.matching = all(f(row) for f in filters)

        class DefaultDict(collections.defaultdict):
            def __missing__(self, k):
                col = Column(k)
                self[k] = col
                return col
        cols = DefaultDict()
        for row in self.rows:
            for key in row:
                col = cols[key]
                col.update(row) # xxx don't update for non-matching rows
        cols = dict(cols)  # un-defaultdict

        def are_duplicates(a, b):
            return (a in cols and b in cols and
                    all(cols[a].cell_for(row) == cols[b].cell_for(row)
                        for row in self.rows))
        self.duplicates = {(a, b) for a, b in DUPLICATES if are_duplicates(a, b)}

        self.unique = {(key, col.unique_value) for key, col in cols.items()
                       if col.unique and key not in ALWAYS_INTERESTING
                      } if len(self.rows) > 1 else {}

        omit = (set(a for a, b in self.duplicates)
                | set(k for k, v in self.unique)
                | set(Row.SYNTHESIZED)
                | set(args.exclude)
                - set(args.include))

        def importance_order(key):
            if key in args.include:
                return (-INF, key)
            else:
                return (IMPORTANCE_ORDER.get(key, INF), key)
        importance = sorted((col for col in cols if col not in omit), key=importance_order)

        remaining_width = args.width_limit
        # pack columns into allotted width (greedy)
        columns = []
        self.overflow = []
        for key in importance:
            col = cols[key]
            if col.width <= remaining_width:
                columns.append(key)
                remaining_width -= col.width + 1
            else:
                self.overflow.append(key)

        self.columns = [cols[k] for k in sorted(columns, key=lambda k: DISPLAY_ORDER.get(k, INF))]

    @staticmethod
    def entity_order_for(host, args):
        """compute entities in row order (each device followed by its partitions)"""
        for device in host.devices_smart_order():
            yield device
            if args.only_devices:
                continue
            if (device.by.get('vdev') or device.zpath) and not args.all_devices:
                continue
            for part in device.partitions:
                assert part.lsblk['PKNAME'] == device.name
                yield part

    @staticmethod
    def filters(args):
        for expr in args.filters:
            m = re.match(r'^([^ =~><!]*)([ =~><!]*)(.*?)$', expr)
            if m:
                lhs, op, rhs = m.groups()
                op = op.strip()
                if op == '=~':   yield Row.comparator_regexp(lhs, rhs)
                elif op == '>=': yield Row.comparator_relative(lhs, operator.ge, op, rhs)
                elif op == '>':  yield Row.comparator_relative(lhs, operator.gt, op, rhs)
                elif op == '<=': yield Row.comparator_relative(lhs, operator.le, op, rhs)
                elif op == '<':  yield Row.comparator_relative(lhs, operator.lt, op, rhs)
                elif op == '!=': yield Row.comparator_direct(lhs, operator.ne, op, rhs)
                elif op == '==': yield Row.comparator_direct(lhs, operator.eq, op, rhs)
                elif op == '=':  yield Row.comparator_direct(lhs, operator.eq, '==', rhs)
                elif op == '':   yield Row.comparator_direct(lhs, lambda a, b: bool(a), 'is set', None)
                else:
                    raise ValueError("couldn't parse filter expression '{}'".format(expr))
            else:
                raise ValueError("couldn't parse filter expression '{}'".format(expr))

    def print_(self):
        if self.duplicates or self.unique:
            lwidth = max(itertools.chain(
                            (len(a) for a, b in self.duplicates),
                            (len(k) for k, v in self.unique),
            ))
            print("Every device has these fields:")
            for a, b in sorted(sorted(self.duplicates), key=lambda k: DISPLAY_ORDER.get(k, INF)):
                print("  {0:{lwidth}} = <{1}>".format(a, b, lwidth=lwidth))
            for k, v in sorted(sorted(self.unique), key=lambda k: DISPLAY_ORDER.get(k, INF)):
                print("  {0:{lwidth}} = {1}".format(k, v, lwidth=lwidth))
            print()

        if self.overflow:
            print("Overflowing labels:\n  {}\n".format(', '.join(
                sorted(self.overflow, key=lambda k: DISPLAY_ORDER.get(k, INF)))))

        if self.filter_log:
            print("Showing only entries where:")
            for f in self.filter_log:
                print("  {}".format(f))
            print()

        # header
        line = ' '.join(col.formatted_cell_for(None, last=False) for col in self.columns)
        print('\033[1m' + line + '\033[0m')

        # rows
        for ii, row in enumerate(self.rows):
            last = ii+1 == len(self.rows) or self.rows[ii+1].indent == False
            line = ' '.join(col.formatted_cell_for(row, last=last) for col in self.columns)
            print('\033[0m' if row.matching else '\033[1;30m', end='')
            print(line)

class Column:
    def __init__(self, key):
        self.key = key
        self.width = len(self.header_cell)
        self.unique = None
        self.unique_value = None

    def update(self, row):
        cell = self.cell_for(row)
        if self.unique is not False:
            if self.unique_value is None:
                self.unique_value = cell
            else:
                self.unique = (self.unique_value == cell)
        cell_len = len(cell) + (len(BOX_END) if row.indent and self.key == 'display_name' else 0)
        self.width = max(self.width, cell_len)

    @property
    def header_cell(self):
        if self.key == 'display_name':
            return 'NAME'  # trick!
        else:
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

    def formatted_cell_for(self, row, *, last): # row=None means header
        if row is None:
            text = self.header_cell
        else:
            text = self.cell_for(row)

        if self.key == 'display_name' and row and row.indent:
            text = (BOX_END if last else BOX_MID) + text

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

class Row:
    SYNTHESIZED = ('NAME', 'PKNAME', 'zpath', 'MOUNTPOINT', 'TYPE', 'vdev', 'SIZE')

    def __init__(self, ent):
        self.ent = ent
        self.short_formatter = bytesize.short_formatter(
            tolerance=0.025,
            try_metric=isinstance(self.ent, data.Device),
        )
        self.matching = True
        self.indent = isinstance(self.ent, data.Partition)

    def __iter__(self):
        yield from ('display_name', 'location', 'zpath', 'size')
        yield from self.ent.lsblk.keys()
        yield from self.ent.by.keys()

    def __getitem__(self, key):
        if key.lower() == 'zpath':
            return self.zpath

        value = self.ent.lsblk.get(key.upper())
        if value:
            return value

        value = self.ent.by.get(key)
        if value:
            return value

        if key.startswith('by-'):
            value = self.by.get(key[3:])
            return value

        raise KeyError("entity '{}' has no key '{}'".format(self.ent.name, key))

    def __contains__(self, key):
        try:
            self[key]
            return True
        except KeyError:
            return False

    def get(self, key, default=None):
        try:
            return self[key]
        except KeyError:
            return default

    def sort_value(self, key):
        if key.lower() == 'size':
            return int(self.ent.lsblk['SIZE'])
        return self.get(key, '')

    @staticmethod
    def comparator_regexp(key, rhs):
        if key.lower() == 'size':
            raise ValueError("can't compare size by regexp")
        def compare(row):
            value = row.get(key, '')
            return re.match(rhs, value)
        return compare, "{} matches regexp /{}/".format(key, rhs)

    @staticmethod
    def comparator_direct(key, op, op_str, rhs):
        if key.lower() == 'size':
            def compare(row):
                return op(row.size, rhs)  # compare to string value as it appears in table
        else:
            def compare(row):
                return op(row.get(key, ''), rhs)

        text = "{} {}".format(key, op_str) + (' {}'.format(rhs) if rhs is not None else '')
        return compare, text

    @staticmethod
    def comparator_relative(key, op, op_str, rhs):
        if key.lower() == 'size':
            return Row.comparator_relative_size(op, op_str, rhs)
        else:
            raise ValueError("can't compare by key '{}'".format(key))

    @staticmethod
    def comparator_relative_size(op, op_str, rhs):
        # parse given size as integer
        try:
            rhs_int = int(rhs)
            def compare(row):
                return op(int(row.ent.lsblk['SIZE']), rhs_int)
            return compare, "SIZE {} {} bytes".format(op_str, rhs_int)
        except ValueError:
            pass

        # parse given size as string with pint/bytesize
        if bytesize.ureg is None:
            raise ValueError("can't parse size '{}' without module 'pint'".format(rhs))

        rhs_q = bytesize.ureg(rhs)
        def compare(row):
            row_size_q = int(row.ent.lsblk['SIZE']) * bytesize.ureg.bytes
            return op(row_size_q, rhs_q)

        text = "SIZE {} {}".format(op_str, rhs_q)
        if rhs_q.magnitude > 1:
            text += 's'

        byte_mag = rhs_q.to('bytes').magnitude
        if not isinstance(byte_mag, int):
            if byte_mag.is_integer():
                byte_mag = int(byte_mag)
            text += " ({} bytes)".format(byte_mag)

        return compare, text

    @property
    def size(self):
        return self.short_formatter(int(self.ent.lsblk['SIZE']))

    @property
    def show_fstype(self):
        return self.ent.lsblk['FSTYPE'] not in ('', 'linux_raid_member', 'zfs_member')

    #xxx
    #def show_BY-vdev-if-partition(self): False

    @property
    def display_name(self):
        lsblk, by = self.ent.lsblk, self.ent.by

        name = lsblk['KNAME']
        if lsblk['NAME'] != name:
            name += '={}'.format(lsblk['NAME'])
        vdev = ('•{}'.format(by['vdev']) if (by.get('vdev') and
                                             not isinstance(self.ent, data.Partition))
                                         else '')
        typ = ('•({})'.format(lsblk['TYPE']) if not (lsblk.get('TYPE')
                                                           in (None, 'disk', 'part', 'md')
                                                         or (lsblk.get('TYPE') == 'loop'
                                                             and self.ent.name.startswith('loop')))
                                           else '')
        return name + vdev + typ

    @property
    def location(self):
        mnt = self.ent.lsblk.get('MOUNTPOINT')
        holders = '[{}]'.format(', '.join(self.ent.holder_names)) if self.ent.holder_names else ''
        assert len(list(filter(None, (self.ent.zpath, mnt)))) <= 1
        return ' '.join(filter(None, (self.ent.zpath, mnt, holders)))

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
    parser.add_argument("-x", "--sort", action='append', dest='sorts', default=[],
                        help="sort entities by field(s); implies --include")
    parser.add_argument("-r", "--reverse", action='store_true', dest='reverse', default=False,
                        help="sort in reverse order")
    parser.add_argument("-w", "--where", action='append', dest='filters', default=[],
                        help="filters e.g. NAME=sdc, vdev=a4")
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

    if args.all_columns:
        args.width_limit = INF
    else:
        try:
            _, width = terminal_size()
            args.width_limit = width - 1
        # xxx if output is not a tty then be sure not to limit width
        except Exception:
            args.width_limit = INF

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

    if host.missing_from_lsblk:
        # xxx more prominent warning (color?)
        print("Present in sysfs but not in `lsblk`:\n  {}\n".format(', '.join(host.missing_from_lsblk)))

    table = Table(host, args)
    table.print_()
