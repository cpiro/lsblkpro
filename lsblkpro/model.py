import operator
import re
import string
import collections

class DeviceCollection:
    # xxx lift non-lsblk data (holders, etc.) all the way out of data module
    def __init__(self, devices, partitions, missing_from_lsblk):
        self._devices = devices
        self._partitions = partitions
        self._missing_from_lsblk = missing_from_lsblk

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

        return sorted(self.devices, key=specified_order)

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

