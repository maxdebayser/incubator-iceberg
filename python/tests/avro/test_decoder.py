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
from datetime import date, datetime, timezone
from decimal import Decimal
from io import SEEK_SET

import pytest

from iceberg.avro.decoder import BinaryDecoder
from iceberg.io.base import InputStream
from iceberg.io.memory import MemoryInputStream


def test_read_decimal_from_fixed():
    mis = MemoryInputStream(b"\x00\x00\x00\x05\x6A\x48\x1C\xFB\x2C\x7C\x50\x00")
    decoder = BinaryDecoder(mis)
    actual = decoder.read_decimal_from_fixed(28, 15, 12)
    expected = Decimal("99892.123400000000000")
    assert actual == expected


def test_read_long():
    mis = MemoryInputStream(b"\x18")
    decoder = BinaryDecoder(mis)
    assert decoder.read_long() == 12


def test_read_decimal():
    mis = MemoryInputStream(b"\x18\x00\x00\x00\x05\x6A\x48\x1C\xFB\x2C\x7C\x50\x00")
    decoder = BinaryDecoder(mis)
    actual = decoder.read_decimal_from_bytes(28, 15)
    expected = Decimal("99892.123400000000000")
    assert actual == expected


def test_decimal_from_fixed_big():
    mis = MemoryInputStream(b"\x0E\xC2\x02\xE9\x06\x16\x33\x49\x77\x67\xA8\x00")
    decoder = BinaryDecoder(mis)
    actual = decoder.read_decimal_from_fixed(28, 15, 12)
    expected = Decimal("4567335489766.998340000000000")
    assert actual == expected


def test_read_negative_bytes():
    mis = MemoryInputStream(b"")
    decoder = BinaryDecoder(mis)

    with pytest.raises(ValueError) as exc_info:
        decoder.read(-1)

    assert f"Requested -1 bytes to read, expected positive integer." in str(exc_info.value)


class OneByteAtATimeInputStream(InputStream):
    """
    Fake input stream that just returns a single byte at the time
    """

    pos = 0

    def read(self, size: int = 0) -> bytes:
        self.pos += 1
        return int.to_bytes(1, self.pos, byteorder="little")

    def seek(self, offset: int, whence: int = SEEK_SET) -> None:
        pass

    def tell(self) -> int:
        pass

    def closed(self) -> bool:
        pass

    def close(self) -> None:
        pass


def test_read_single_byte_at_the_time():
    decoder = BinaryDecoder(OneByteAtATimeInputStream())

    with pytest.raises(ValueError) as exc_info:
        decoder.read(2)

    assert f"Read 1 bytes, expected 2 bytes" in str(exc_info.value)


def test_read_float():
    mis = MemoryInputStream(b"\x00\x00\x9A\x41")
    decoder = BinaryDecoder(mis)
    assert decoder.read_float() == 19.25


def test_read_double():
    mis = MemoryInputStream(b"\x00\x00\x00\x00\x00\x40\x33\x40")
    decoder = BinaryDecoder(mis)
    assert decoder.read_double() == 19.25


def test_read_date():
    mis = MemoryInputStream(b"\xBC\x7D")
    decoder = BinaryDecoder(mis)
    assert decoder.read_date_from_int() == date(1991, 12, 27)


def test_read_time_millis():
    mis = MemoryInputStream(b"\xBC\x7D")
    decoder = BinaryDecoder(mis)
    assert decoder.read_time_millis().microsecond == 30000


def test_read_time_micros():
    mis = MemoryInputStream(b"\xBC\x7D")
    decoder = BinaryDecoder(mis)
    assert decoder.read_time_micros().microsecond == 8030


def test_read_timestamp_micros():
    mis = MemoryInputStream(b"\xBC\x7D")
    decoder = BinaryDecoder(mis)
    assert decoder.read_timestamp_micros() == datetime(1970, 1, 1, 0, 0, 0, 8030)


def test_read_timestamptz_micros():
    mis = MemoryInputStream(b"\xBC\x7D")
    decoder = BinaryDecoder(mis)
    assert decoder.read_timestamptz_micros() == datetime(1970, 1, 1, 0, 0, 0, 8030, tzinfo=timezone.utc)
