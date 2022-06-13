# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
from __future__ import annotations

import binascii
import struct

from iceberg.avro.codecs.codec import Codec
from iceberg.avro.decoder import BinaryDecoder
from iceberg.io.memory import MemoryInputStream

STRUCT_CRC32 = struct.Struct(">I")  # big-endian unsigned int

try:
    import snappy

    class SnappyCodec(Codec):
        @staticmethod
        def _check_crc32(bytes_: bytes, checksum: bytes) -> None:
            """Incrementally compute CRC-32 from bytes and compare to a checksum

            Args:
              bytes_ (bytes): The bytes to check against `checksum`
              checksum (bytes): Byte representation of a checksum

            Raises:
              ValueError: If the computed CRC-32 does not match the checksum
            """
            if binascii.crc32(bytes_) & 0xFFFFFFFF != STRUCT_CRC32.unpack(checksum)[0]:
                raise ValueError("Checksum failure")

        @staticmethod
        def compress(data: bytes) -> tuple[bytes, int]:
            compressed_data = snappy.compress(data)
            # A 4-byte, big-endian CRC32 checksum
            compressed_data += STRUCT_CRC32.pack(binascii.crc32(data) & 0xFFFFFFFF)
            return compressed_data, len(compressed_data)

        @staticmethod
        def decompress(readers_decoder: BinaryDecoder) -> BinaryDecoder:
            # Compressed data includes a 4-byte CRC32 checksum
            length = readers_decoder.read_long()
            data = readers_decoder.read(length - 4)
            uncompressed = snappy.decompress(data)
            checksum = readers_decoder.read(4)
            SnappyCodec._check_crc32(uncompressed, checksum)
            return BinaryDecoder(MemoryInputStream(uncompressed))

except ImportError as ex:

    class SnappyCodec(Codec):  # type: ignore
        @staticmethod
        def compress(data: bytes) -> tuple[bytes, int]:
            raise ImportError("Snappy support not installed, please install using `pip install pyiceberg[snappy]`")

        @staticmethod
        def decompress(readers_decoder: BinaryDecoder) -> BinaryDecoder:
            raise ImportError("Snappy support not installed, please install using `pip install pyiceberg[snappy]`")
