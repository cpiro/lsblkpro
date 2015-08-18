#!/usr/bin/env python3.4

# TODO prioritize fields based on term width, mix in `lsscsi`, scsi path, sas
# addr/phy, everything
# TODO attach time
# xxx package desc should include /sys/block

import os
import sys
import subprocess
import re
import operator
import logging
import argparse
import itertools
import operator

import struct
import fcntl
import termios

import pprint
pp = pprint.pprint

from . import data

ALWAYS_INTERESTING = frozenset('SIZE')

IMPORTANCE = [
    'displayname',
    'location',

    'name',
    'KNAME',
    'by-vdev',
    'zpath',
    'MOUNTPOINT',
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
    'ALIGNMENT',
    'OPT-IO',
    'TYPE',
    'ROTA',
    'MODEL',
    'RO',
    'RM',
    'by-partlabel',
    'by-path',
    'by-id',
    'UUID',
    'SERIAL',
    'MIN-IO',
    'PHY-SEC',
    'LOG-SEC',
    'WWN',
    'PARTUUID',
    'PARTTYPE',
    'PARTLABEL',
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

def terminal_size():
    h, w, hp, wp = struct.unpack('HHHH',
                       fcntl.ioctl(0, termios.TIOCGWINSZ,
                           struct.pack('HHHH', 0, 0, 0, 0)))
    return h, w

def value_to_str(r, l):
    if l == 'MAJ:MIN':  # align the colons
        v = ' ' * (3-r[l].index(':')) + r[l]
        return v + ' ' * (7-len(v))
    elif l in r:
        return str(r[l])
    else:
        return ''

def width_for_column(label, rows):
    return max(
        len(header(label, label)),
        max(len(value_to_str(row,label)) for row in rows)
    )

def header(_, l, width=None):
    if l == 'displayname':
        return 'DEVICE'
    elif l == 'location':
        return ''
    elif l.startswith('by-'):
        return l[3:]
    else:
        return l


def main():
    args = {'all': False}

    # xxx pull in long descriptions from lsblk, somehow

    devices, partitions = data.get_data(args)
    # xxx optionally not filter zpool drive partitions
    not_zpool_partitions = {k: p for k, p in partitions.items() if not devices[p['PKNAME']].get('zpath') }

    # compute rows (each device followed by its partitions)
    rows = []
    for device in sorted(devices.values(), key=operator.itemgetter('name')):
        rows.append(device)
        if device.get('zpath'):
            continue
        for partname in device['partitions']:
            part = partitions[partname]
            assert part['PKNAME'] == device['name']
            rows.append(part)

    # labels
    all_labels = set()
    for row in rows:  # victoresque
        all_labels |= row.keys()

    # munge
    def display_name_for(row, *, last):
        vdev = '•{}'.format(row['by-vdev']) if (row.get('by-vdev') and
                                                row['name'] not in partitions) else ''
        typ = '•({})'.format(row['TYPE']) if row['TYPE'] not in ('disk', 'part', 'md') else ''

        if row['name'] in partitions:
            box = ' └─ ' if last else ' ├─ '
            return box + row['name'] + vdev + typ
        else:
            return row['name'] + vdev + typ

    def location_for(row):
        zpath = row.get('zpath', '')
        mnt = row.get('MOUNTPOINT', '')
        holders = '[{}]'.format(', '.join(row['holders'])) if row.get('holders') else ''

        assert not (zpath and mnt)
        return ' '.join(x for x in (zpath, mnt, holders) if x)

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

    # figure out labels
    omit = {
        'PKNAME', 'name', 'zpath', 'MOUNTPOINT', 'TYPE', 'by-vdev', 'holders', 'partitions', # used by munge
        'major', 'minor',  'size', # xxx
        'MODEL', # boring
    }

    every_device_has = []
    for candidate, reference in (('KNAME', 'name'), ):
        if all(row[candidate] == row[reference] for row in rows):
            every_device_has.append((candidate, '<{}>'.format(reference)))
            omit.add(candidate)

    width_label_pairs = []
    overflow = []
    running_width = 0
    _, width_limit = terminal_size()
    width_limit -= 1

    for label in IMPORTANCE:
        if label in omit:
            continue

        values_in_this_column = set(value_to_str(r, label) for r in rows)
        if (label in ALWAYS_INTERESTING or
            not (len(rows) == 1 or len(values_in_this_column) == 1)):
            width = width_for_column(label, rows)

            if running_width + width > width_limit:
                overflow.append(label)
            else:
                running_width += width + 1
                width_label_pairs.append((width, label))
        else:
            val = values_in_this_column.pop()
            every_device_has.append((label, val))

    # pre-print
    if every_device_has:
        print("Every device has these fields:")
        lwidth = max(len(l) for l, _ in every_device_has)
        for l, v in every_device_has:
            print("  {0:{lwidth}} = {1}".format(l, v, lwidth=lwidth))
        print()

    missing_labels = all_labels - set(IMPORTANCE) - omit
    if missing_labels:
        print("Missing labels:\n  {}\n".format(sorted(missing_labels)))

    if overflow:
        print("Overflowing labels:\n  {}\n".format(sorted(overflow)))

    # print
    def order(elt):
        w, l = elt
        if l in SORT_ORDER:
            return SORT_ORDER[l]
        else:
            return w + 1000 # shorter ones first

    width_label_pairs = sorted(width_label_pairs, key=order)
    print_table(width_label_pairs, rows, [])


###

def print_table(width_label_pairs, rows, highlights):
    format_options = {
        #'name': '<',
        'displayname': '<',
        'location': '<',
        #'zpool': '>',
        'TRAN': '>',
        'HCTL': '<',
        #'by-vdev': '>',
        'by-id': '<',
        'by-path': '<',
        #'MOUNTPOINT': '<',
        }

    def format(l, w):
        fmt = format_options.get(l)
        if fmt in ('>', '<'):
            fmt += str(w)
        elif fmt is None:
            fmt = '>' + str(w)
        return fmt

    def value_to_str_bullets(r, l, width):
        s = value_to_str(r, l)
        if '•' in s:
            a, b = s.split('•')
            sep = ' ' * (width - len(a) - len(b))
            return a + sep + b
        else:
            return s

    def print_row(r, xform):
        cells = ("{0:{fmt}}".format(xform(r,l,width=w), fmt=format(l,w))
                 for w, l in width_label_pairs)
        line = ' '.join(cells)

        color = r.get('$color')
        if color is not None:
            line = color + line + '\033[0m'

        print(line)

    print_row({l: l for w, l in width_label_pairs}, header)
    for r in rows:
        print_row(r, value_to_str_bullets)

def apply_filters(devices, filters):
    for f in filters:
        try:
            if '=~' in f:
                lhs, rhs = f.split('=~', 1)
                logging.debug("Showing only devices where %s matches /%s/", lhs, rhs)
                devices = [d for d in devices if re.match(rhs, d[lhs])]
            elif '!=' in f:
                lhs, rhs = f.split('!=', 1)
                logging.debug("Showing only devices where %s != '%s'", lhs, rhs)
                devices = [d for d in devices if d[lhs] != rhs]
            elif '=' in f:
                lhs, rhs = f.split('=', 1)
                logging.debug("Showing only devices where %s == '%s'", lhs, rhs)
                devices = [d for d in devices if d[lhs] == rhs]
            else:
                logging.error("fmp")
                sys.exit(0)
        except KeyError as ex:
            logging.error("no such key '%s'", lhs)
            sys.exit(0)
    return devices

def find_highlights(devices, highlight):
    if highlight is None:
        return {}
    # i'm colorblind gimme a break
    color_list = ['0', '31', '32', '34', '41', '42', '44', '45', '30;46', '30;47']
    color_table = {}  # value => color
    color = None
    for d in devices:
        if d[highlight] not in color_table:
            try:
                color = color_list.pop(0)
                color_table[d[highlight]] = "\033[{}m".format(color)
            except IndexError:
                logging.error("too many colors")
                sys.exit(0)
        d['$color'] = color_table[d[highlight]]

def old_main():
    if not sys.platform.startswith('linux'):
        logging.error("You're gonna want that Linux")
        sys.exit(1)

    parser = argparse.ArgumentParser()
    parser.add_argument("-a", "--all", action='store_true',
                        help="print all devices (passed through to lsblk)")
    parser.add_argument("-b", "--by", action='append', dest='lookups', default=[],
                        help="look up alternate names in /dev/disk/<arg>")
    parser.add_argument("-s", "--sort", action='append', dest='sorts', default=[],
                        help="sort by field(s)")
    parser.add_argument("-w", "--where", action='append', dest='filters', default=[],
                        help="filters e.g. NAME=sdc, zpool=a4")
    parser.add_argument("-i", "--highlight",
                        help="highlight entries by a field")
    parser.add_argument("-p", "--partitions", action='store_true',
                        help="show all partitions")
    parser.add_argument("-z", "--zpool", action='store_true',
                        help="equiv. to: -w 'zpool!=' -s zpool -i SIZE")
    args = parser.parse_args()

    if args.zpool:
        success = False
        if os.path.exists('/dev/disk/zpool'):
            args.filters.append('zpool!=')
            args.sorts[0:0] = ['zpool']
            success = True
        if os.path.exists('/dev/disk/by-vdev'):
            args.filters.append('by-vdev!=')
            args.sorts[0:0] = ['by-vdev']
            success = True
        if not success:
            logging.error("/dev/disk/{zpool,by-vdev} not found")
            sys.exit(1)
        if args.highlight is None:
            args.highlight = 'SIZE'

    labels = ['NAME','MOUNTPOINT','MAJ:MIN','RO','RM','SIZE','OWNER','GROUP','MODE','ALIGNMENT','MIN-IO','OPT-IO','PHY-SEC','LOG-SEC','ROTA','TYPE', 'MODEL', 'STATE', 'LABEL', 'FSTYPE'] # 'UUID' xxx

    import itertools
    def uniq(iterable):
        for k, _ in itertools.groupby(iterable):
            yield k

    results = list(uniq(lsblk(labels, args)))

    if args.partitions:
        top_level_devices = results
    else:
        top_level_devices = [d for d in results if d['TYPE'] != 'part' or d['MOUNTPOINT'] != '']

    format_options = {}

    lookups = args.lookups
    silent_lookups = []
    if os.path.exists('/dev/disk/zpool') and 'zpool' not in lookups:
        lookups[0:0] = ['zpool']
    if os.path.exists('/dev/disk/by-vdev') and 'by-vdev' not in lookups:
        lookups[0:0] = ['by-vdev']
    if os.path.exists('/dev/disk/by-id') and 'by-id' not in lookups:
        silent_lookups.append('by-id')
    for kind in lookups + silent_lookups:
        by_dev_disk(kind, top_level_devices)
        format_options[kind] = '<'
    labels[1:1] = lookups

    devices = apply_filters(top_level_devices, args.filters)
    if not devices:
        print("no matches among {} devices".format(len(top_level_devices)))
        sys.exit(0)

    # punch up with zpool status, if we can get it without prompting for a password
    try:
        zpool_status = subprocess.check_output(['sudo', '-n', 'zpool', 'status'], stderr=subprocess.STDOUT)
        zpaths = parse_zpool_status(zpool_status)
        if all(v.endswith('-0') for v in zpaths.values()):
            zpaths = {k: v[0:-2] for k, v in zpaths.items()}
        for d in devices:
            vdev = d.get('by-vdev')
            idd = d.get('by-id')
            if vdev and vdev in zpaths:
                d['MOUNTPOINT'] = zpaths[vdev]
            elif idd and idd in zpaths:
                d['MOUNTPOINT'] = zpaths[idd]
    except subprocess.CalledProcessError as ex:
        if ex.output == 'sudo: a password is required\n' and ex.returncode == 1:
            print("WARNING: couldn't get zpool status non-interactively; consider adding this to sudoers:\n")
            print("    {} ALL=NOPASSWD: /sbin/zpool status\n".format(os.environ['USER']))
        else:
            logging.exception(ex)
            print()

    highlights = find_highlights(devices, args.highlight)

    sorts = args.sorts
    sorts.append('NAME')

    logging.debug("Sorting by %s", ', '.join(sorts))
    rows = sorted(devices, key=operator.itemgetter(*sorts))

    labels, uninteresting = pull_uninteresting(labels, rows)

    if uninteresting:
        print("Every device has these fields:")
        lwidth = max(len(l) for l, _ in uninteresting)
        for l, v in uninteresting:
            print("  {0:{lwidth}} = {1}".format(l, v, lwidth=lwidth))
        print()

    print_table(labels, rows, highlights)
