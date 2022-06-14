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

from io import BytesIO

from iceberg.avro.codecs.codec import Codec

try:
    from zstandard import ZstdCompressor, ZstdDecompressor

    class ZStandardCodec(Codec):
        @staticmethod
        def compress(data: bytes) -> tuple[bytes, int]:
            compressed_data = ZstdCompressor().compress(data)
            return compressed_data, len(compressed_data)

        @staticmethod
        def decompress(data: bytes) -> bytes:
            uncompressed = bytearray()
            dctx = ZstdDecompressor()
            with dctx.stream_reader(BytesIO(data)) as reader:
                while True:
                    chunk = reader.read(16384)
                    if not chunk:
                        break
                    uncompressed.extend(chunk)
            return uncompressed

except ImportError as ex:

    class ZStandardCodec(Codec):  # type: ignore
        @staticmethod
        def compress(data: bytes) -> tuple[bytes, int]:
            raise ImportError("Zstandard support not installed, please install using `pip install pyiceberg[zstandard]`")

        @staticmethod
        def decompress(data: bytes) -> bytes:
            raise ImportError("Zstandard support not installed, please install using `pip install pyiceberg[zstandard]`")
