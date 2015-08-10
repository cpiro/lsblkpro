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
import ctypes
import itertools

import pprint
pp = pprint.pprint

CLI_UTILS_ENCODING = 'utf-8'


def dev_name_split(device):
    def to_int_maybe(p):
        try:
            return int(p)
        except ValueError:
            return p

    return tuple(to_int_maybe(part) for part in re.findall(r'(?:[a-z]+|\d+)', device))

def top_level_devices(args):
    for device in os.listdir(os.path.join('/sys', 'block')):
        if args['all'] or not re.fullmatch(r'(?:ram\d+|loop\d+)', device):
            yield device

def is_partition_dirent(device, directory):
    if not directory.startswith(device):
        return False
    return os.path.exists(os.path.join('/sys', 'block', device, directory, 'start'))

def read_sysfs(path, filename):
    with open(os.path.join(path, filename), 'r') as f:
        data = f.read()
    try:
        return int(data)
    except ValueError:
        return data

def to_bool(zero_or_one):
    assert zero_or_one == 0 or zero_or_one == 1
    return bool(zero_or_one)

def parse_maj_min(data, row):
    m = re.match(r'(\d*):(\d*)', data)
    assert m
    row['major'] = int(m.group(1))
    row['minor'] = int(m.group(2))

def walk_device(device):
    row = {'name': device, 'partitions': []}
    path = os.path.join('/sys', 'block', device)
    for entry in os.listdir(path):
        if is_partition_dirent(device, entry):
            row['partitions'].append(entry)
        elif entry == 'holders':
            holders = os.listdir(os.path.join('/sys', 'block', device, 'holders'))
            if holders:
                row['holders'] = holders
        elif entry == 'dev':
            parse_maj_min(read_sysfs(path, entry), row)
        elif entry in ('size',):
            row[entry] = int(read_sysfs(path, entry))

    return row

def walk_partition(device, part):
    row = {'name': part}
    path = os.path.join('/sys', 'block', device, part)
    entries = os.listdir(path)
    for entry in entries:
        if entry == 'holders':
            holders = os.listdir(os.path.join('/sys', 'block', device, part, 'holders'))
            if holders:
                row['holders'] = holders
        elif entry == 'dev':
            parse_maj_min(read_sysfs(path, entry), row)
    return row

def main():
    args = {'all': False}

    # sysfs
    devices = []
    partitions = []
    for device_name in top_level_devices(args):
        row = walk_device(device_name)
        devices.append(row)
        for part_name in row['partitions']:
            partitions.append(walk_partition(device_name, part_name))

    devices = {d['name']: d for d in devices}
    partitions = {p['name']: p for p in partitions}

    # lsblk
    results = lsblk(None, args)
    def merge_row(row, result):
        row.update(result)
        del row['NAME']
        assert '{}:{}'.format(row['major'], row['minor']) == row['MAJ:MIN']
        #del row['MAJ:MIN']

    for result in results:
        name = result['NAME']
        if name in devices:
            merge_row(devices[name], result)
        elif name in partitions:
            merge_row(partitions[name], result)
        else:
            assert False, result # xxx

    # /dev/disk
    for kind in os.listdir(os.path.join('/dev', 'disk')):
        path = os.path.join('/dev', 'disk', kind)
        for fn in os.listdir(path):
            dev_or_part = os.path.basename(os.readlink(os.path.join(path, fn)))

            if dev_or_part in devices:
                row = devices[dev_or_part]
            elif dev_or_part in partitions:
                row = partitions[dev_or_part]
            else:
                assert False, (kind, fn, dev_or_part)

            if kind == 'by-partuuid':
                assert row['PARTUUID'] == fn
            elif kind == 'by-uuid':
                assert row['UUID'] == fn
            else:
                row[kind] = fn

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

    # labels
    all_labels = set()
    for row in itertools.chain(devices.values()): #, partitions.values()):
        all_labels |= row.keys()

    #pp(devices)
    #pp(partitions)
    #pp(labels)

    labels = ['name','MOUNTPOINT','MAJ:MIN','RO','RM','SIZE','OWNER','GROUP','MODE','ALIGNMENT','MIN-IO','OPT-IO','PHY-SEC','LOG-SEC','ROTA','TYPE', 'MODEL', 'STATE', 'LABEL', 'FSTYPE'] # 'UUID' xxx
    missing_labels = all_labels - set(labels)


    import operator
    rows = sorted(devices.values(), key=operator.itemgetter('name'))
    labels, uninteresting = pull_uninteresting(labels, rows)

    if uninteresting:
        print("Every device has these fields:")
        lwidth = max(len(l) for l, _ in uninteresting)
        for l, v in uninteresting:
            print("  {0:{lwidth}} = {1}".format(l, v, lwidth=lwidth))
        print()

    if missing_labels:
        print("missing labels: {}".format(sorted(missing_labels)))


    print_table(labels, rows, [])

###

def lsblk(labels, args):
    cmd = ['lsblk']
    if args['all']:
        cmd.append('--all')
    cmd.extend(['-P', '-O'])
    out = subprocess.check_output(cmd)
    results = []
    for l in out.decode(CLI_UTILS_ENCODING).splitlines():
        a = re.findall(r'(.*?)="(.*?)" ?', l)
        d = {k:v for k,v in a}
        results.append(d)
    return results

def by_dev_disk(kind, results):
    try:
        path = os.path.join('/dev', 'disk', kind)
        devices = os.listdir(path)
    except OSError as ex:
        logging.warning("can't sort by %s; no such directory", path)
        for r in results:
            r[kind] = ''
        return

    def lookup(device):
        path = os.path.join('/dev', 'disk', kind, device)
        link = os.readlink(path)
        return os.path.basename(link)

    mapp = {lookup(d): d for d in devices}

    for r in results:
        r[kind] = mapp.get(r['NAME'], '')

def print_table(labels, rows, highlights):
    format_options = {
        'NAME': '<',
        'zpool': '>',
        'by-vdev': '>',
        'by-id': '<',
        'by-path': '<',
        'MOUNTPOINT': '<',
        }

    def format(l, w):
        fmt = format_options.get(l)
        if fmt in ('>', '<'):
            fmt += str(w)
        elif fmt is None:
            fmt = '>' + str(w)
        return fmt

    def header(_, l):
        if l.startswith('by-'):
            return l[3:]
        else:
            return l

    def value(r, l):
        if l == 'MAJ:MIN':  # align the colons
            v = ' ' * (3-r[l].index(':')) + r[l]
            return v + ' ' * (7-len(v))
        elif l in r:
            return str(r[l])
        else:
            return '???'

    # column widths
    widths = [ max(
                   len( header(l,l) ),
                   max([len( value(r,l) ) for r in rows]))
               for l in labels]

    def print_row(r, xform):
        cells = ("{0:{fmt}}".format(xform(r,l), fmt=format(l,w))
                 for l, w in zip(labels, widths))
        line = ' '.join(cells)

        color = r.get('$color')
        if color is not None:
            line = color + line + '\033[0m'

        print(line)

    print_row({l:l for l in labels}, header)
    for r in rows:
        print_row(r, value)

def pull_uninteresting(labels, rows):
    interesting = []
    uninteresting = []

    for l in labels:
        values_in_this_column = set(r.get(l) for r in rows if r.get(l))
        if l == 'SIZE':
            interesting.append(l)
        elif len(rows) == 1 or len(values_in_this_column) == 1:
            val = values_in_this_column.pop()
            if len(val) > 0:
                uninteresting.append((l, val))
        else:
            interesting.append(l)
    return interesting, uninteresting

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
