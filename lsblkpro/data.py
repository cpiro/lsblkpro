import os
import sys
import re
import subprocess

CLI_UTILS_ENCODING = sys.stdout.encoding

def lsblk(args):
    cmd = ['lsblk']
    if args.all_devices:
        cmd.append('--all')
    cmd.extend(['-P', '-O'])
    out = subprocess.check_output(cmd)
    results = []
    for l in out.decode(CLI_UTILS_ENCODING).splitlines():
        a = re.findall(r'(.*?)="(.*?)" ?', l)
        d = {k:v for k,v in a}
        results.append(d)
    return results

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

def top_level_devices(args):
    for device in os.listdir(os.path.join('/sys', 'block')):
        if args.all_devices or not re.fullmatch(r'(?:ram\d+|loop\d+)', device):
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

# def to_bool(zero_or_one):
#     assert zero_or_one == 0 or zero_or_one == 1
#     return bool(zero_or_one)

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

def get_data(args):
    # sysfs
    device_list = []
    partition_list = []
    sysfs_names = set()
    for device_name in top_level_devices(args):
        row = walk_device(device_name)
        device_list.append(row)
        sysfs_names.add(row['name'])
        for part_name in row['partitions']:
            row = walk_partition(device_name, part_name)
            partition_list.append(row)
            sysfs_names.add(row['name'])

    # lsblk
    results = lsblk(args)

    lsblk_names = set()
    for result in results:
        name = result['NAME']
        lsblk_names.add(name)
        if not name in sysfs_names:
            raise RuntimeError("device '{}' in lsblk results not in /sys/block/*/*".format(name))

    # xxx rather than exclude these, use maj:min from sysfs
    missing_from_lsblk = sysfs_names - lsblk_names
    devices = {d['name']: d for d in device_list
                            if d['name'] in lsblk_names}
    partitions = {p['name']: p for p in partition_list
                               if p['name'] in lsblk_names}

    def merge_row(row, result):
        row.update(result)
        #del row['NAME']
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

    return devices, partitions, missing_from_lsblk
