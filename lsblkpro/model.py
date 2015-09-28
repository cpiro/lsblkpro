import re

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


class Device:
    def __init__(self, name, *, collection):
        self.name = name
        self.collection = collection

    @property
    def data(self):
        return self.collection._devices[self.name]

    @property
    def partitions(self):
        for partname in self.data['partitions']:
            _part = self.collection._partitions[partname]
            assert _part['PKNAME'] == self.name
            assert _part['name'] == partname
            yield self.collection._partitions_by_name[partname]

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

