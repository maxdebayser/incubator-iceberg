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
import asyncio
from itertools import chain
from typing import Iterable, List, Optional

from s3fs import S3FileSystem

from pyiceberg.expressions.base import BooleanExpression
from pyiceberg.io import FileIO
from pyiceberg.io.fsspec import _get_signer
from pyiceberg.manifest import ManifestEntry, read_manifest_entry, read_manifest_list
from pyiceberg.schema import Schema
from pyiceberg.table import Snapshot, Table


class TableScan:
    table: Table
    snapshot: Snapshot
    schema: Schema

    def __init__(
            self,
            table: Table,
            snapshot_id: Optional[int] = None,
            schema: Optional[Schema] = None,
            filter_expr: Optional[BooleanExpression] = None,
    ):
        self.table = table
        if snapshot_id is None:
            if table.metadata.current_snapshot_id:
                snapshot_id = table.metadata.current_snapshot_id
            else:
                raise ValueError("Could not determine the current snapshot")

        snapshot = table.snapshot_by_id(snapshot_id)

        if snapshot is None:
            raise ValueError(f"Could not find the snapshot: {snapshot_id}")

        self.snapshot = snapshot

        if schema is None:
            schema = next(schema for schema in table.metadata.schemas if schema.schema_id == snapshot.schema_id)

        self.schema = schema

        if filter_expr:
            # Waiting for ManifestEvaluator https://github.com/apache/iceberg/pull/5845
            raise NotImplementedError("Filtering not yet available")

    @property
    def files(self) -> List[str]:
        io = self.table.io()
        files = []
        if manifest_list := self.snapshot.manifest_list:
            files = [file.data_file.file_path for file in self.fetch_data_files(manifest_list, io)]

        import pyarrow.dataset as ds

        io = self.table.io()
        if not isinstance(io, S3FileSystem):
            raise NotImplementedError("Currently we only support S3FS")

        ds.dataset("data/", filesystem=io)

        dataset = ds.FileSystemDataset.from_paths(
            ["data_2018.parquet", "data_2019.parquet"],
            format=ds.ParquetFileFormat(),
            filesystem=io,
            # To be implemented, would be really cool
            #partitions=[ds.field('year') == 2018, ds.field('year') == 2019]
        )

        return files


    def fetch_data_files(self, manifest_path: str, io: FileIO) -> Iterable[ManifestEntry]:
        files = asyncio.run(self.fetch_manifest(manifest_path, io))
        return chain.from_iterable(files)

    async def fetch_manifest_entries(self, manifest_path: str, io: FileIO) -> List[ManifestEntry]:
        file = io.new_input(manifest_path)
        return list(read_manifest_entry(file))

    async def fetch_manifest(self, manifest_list: str, io: FileIO) -> List[List[ManifestEntry]]:
        file = io.new_input(manifest_list)
        return [await self.fetch_manifest_entries(manifest.manifest_path, io) for manifest in read_manifest_list(file)]

