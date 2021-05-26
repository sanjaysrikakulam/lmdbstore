"""LMDB backed key-value store
"""

import os
import sys
import lmdb
import msgpack
import psutil
import zstd


class LMDBStore:
    """LMDB backed key-value store

    Args:
      path: Directory path or file name prefix where this DB is (to be) stored
      map_size: Maximum size database may grow to, in bytes [2199023255552]
      readonly: Whether to open the LMDB as read only [False]
      lock: Whether to lock LMDB during concurrent writing [True]
      **kwargs: Keyword arguments passed through to the 'lmdb.open' function

    Notes:
      Map size is set to 2 TiB assuming it to be used on 64-bit Linux OS, change it to <2GB for 32-bit
      MessagePack is used for serializing keys and values
      Zstandard is used for compressing the serialized values
      Flush and close methods must be called exclusively to make sure that the writes are synced to the disk
    """

    def __init__(
        self, path, map_size=2199023255552, readonly=False, lock=True, **kwargs
    ):
        # Set LMDB defaults and user provided config to 'lmdb.open'
        # Default map size is set to 2TiB (only on 64 bit systems, should be < 2GB for 32 bit systems)
        kwargs.setdefault("map_size", map_size)
        kwargs.setdefault("readonly", readonly)
        kwargs.setdefault("metasync", False)
        kwargs.setdefault("sync", False)

        # Enable writemap only on Linux systems
        # LMDB doc: This option may cause filesystems that don’t support
        # sparse files, such as OSX, to immediately preallocate map_size= bytes
        # of underlying storage when the environment is opened or closed for
        # the first time.
        if sys.platform.startswith("linux"):
            kwargs.setdefault("writemap", True)

        kwargs.setdefault("map_async", False)
        # kwargs.setdefault("max_dbs", len(db_names))
        kwargs.setdefault("max_spare_txns", psutil.cpu_count())
        kwargs.setdefault("lock", lock)

        path = os.path.abspath(path)
        self.env = lmdb.open(path, **kwargs)

    def __getitem__(self, key):
        """Get item from the DB

        Args:
          key: key

        Returns: Value if key is present else an empty list is returned as a default value

        """
        with self.env.begin() as txn:
            value = txn.get(self.serialize(key))
        return self.decompress(value)

    def __setitem__(self, key, value):
        """Set item in the DB

        Args:
          key: key
          value: value to set

        """
        with self.env.begin(write=True) as txn:
            txn.put(self.serialize(key), self.compress(value))

    def __delitem__(self, key):
        """Delete item from the DB

        Args:
          key: key to delete

        """
        with self.env.begin(write=True) as txn:
            if not txn.delete(self.serialize(key)):
                raise KeyError(key)

    def __contains__(self, key):
        """Membership testing (using 'in' and 'not in')

        Args:
          key: key

        Returns: True if key exists else False
        """
        with self.env.begin() as txn:
            with txn.cursor() as cursor:
                return cursor.set_key(self.serialize(key))

    def __enter__(self):
        """with statement context managers"""
        return self

    def __exit__(self, *args):
        """with statement context managers"""
        self.close()

    def __len__(self):
        """Total number of items present in the DB"""
        return self.env.stat()["entries"]

    def serialize(self, item):
        """Serialization using MessagePack

        Args:
          item: Any item (int, str, list, dict) to serialize

        Returns: Serialized object

        """
        return msgpack.dumps(item)

    def deserialize(self, item):
        """Deserialization using MessagePack

        Args:
          item: Any MessagePack serialized object

        Returns: Deserialized object

        """
        return msgpack.loads(item)

    def compress(self, value):
        """Serialize and Zstandard compress the value

        Args:
          value: Any value, could be list, dict, string, int

        Returns: Compressed serialized bytes

        """
        return zstd.compress(self.serialize(value), 9, 2)

    def decompress(self, compressed_value):
        """Zstandard decompress and deserialize the compressed value

        Args:
          compressed_value: Any bytes that was compressed using 'compress' function

        Returns: Decompressed and deserialized value if not None, else empty list as default value is returned

        """
        if not compressed_value:
            return list()
        return self.deserialize(zstd.decompress(compressed_value))

    def keys(self):
        """To get all the keys stored under the DB

        Returns: Generator object to return keys using a forward iterator

        """
        with self.env.begin() as txn:
            with txn.cursor() as cursor:
                for key in cursor.iternext(keys=True, values=False):
                    yield self.deserialize(key)

    def values(self):
        """To get all the values stored under the DB

        Returns: Generator object to return values using a forward iterator

        """
        with self.env.begin() as txn:
            with txn.cursor() as cursor:
                for value in cursor.iternext(keys=False, values=True):
                    yield self.decompress(value)

    def items(self):
        """Get all key, value pairs stored under the DB

        Returns: Generator object to return key, value pairs using a forward iterator

        """
        with self.env.begin() as txn:
            with txn.cursor() as cursor:
                for key, value in cursor.iternext(keys=True, values=True):
                    yield self.deserialize(key), self.decompress(value)

    def get_multi(self, keys):
        """Get values for multiple keys

        Args:
          keys: List of keys

        Returns: Generator object for values

        """
        with self.env.begin() as txn:
            with txn.cursor() as cursor:
                for key in keys:
                    yield self.decompress(cursor.get(self.serialize(key)))

    def set_multi(self, keys, values):
        """Set multiple key, value pairs at once

        Args:
          keys: List of keys
          values: list of values

        Note:
          If key already exists, the value will be appended, if not the value will be added as a list

        """
        if len(keys) != len(values):
            raise ValueError("Number of keys are not equal to number of values")

        with self.env.begin(write=True) as txn:
            with txn.cursor() as cursor:
                for key, value in zip(keys, values):
                    val = self.decompress(cursor.get(self.serialize(key)))
                    val.append(value)
                    cursor.put(self.serialize(key), self.compress(val))

    def flush(self):
        """Sync/Write data to the file system"""
        self.env.sync()

    def close(self):
        """Close the DB"""
        self.env.close()

    def create_named_database(self):
        """Creates named databases

        Note: Currently not implemented
        """
        raise NotImplementedError
