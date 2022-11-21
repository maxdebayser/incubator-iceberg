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
from pyiceberg.catalog import load_catalog
from pyiceberg.expressions import EqualTo

# python3 -m cProfile -o test.pstats  -s tottime tests/test_performance.py
# python3 -m gprof2dot -f pstats test.pstats | dot -Tpng -o output.png

assert [
    file.file.file_path
    for file in load_catalog("local", type="rest")
    .load_table(("nyc", "taxis"))
    .scan()
    .filter_partitions(EqualTo("VendorID", 5))
    .plan_files()
] == [
    "s3a://warehouse/wh/nyc/taxis/data/VendorID=5/00003-506-c9e4e601-f928-4527-bfd2-5331347d5020-00003.parquet",
    "s3a://warehouse/wh/nyc/taxis/data/VendorID=5/00003-497-5482509f-866d-466f-9fc3-5ea9699a8418-00003.parquet",
    "s3a://warehouse/wh/nyc/taxis/data/VendorID=5/00003-488-4c01921b-7f5a-4237-926f-c3493ab043b3-00003.parquet",
    "s3a://warehouse/wh/nyc/taxis/data/VendorID=5/00003-479-286951ca-66ac-46fa-9d48-68ee9901c241-00003.parquet",
    "s3a://warehouse/wh/nyc/taxis/data/VendorID=5/00003-461-3c2c0cc4-7dbd-41f7-be16-7a3f9ac21717-00003.parquet",
    "s3a://warehouse/wh/nyc/taxis/data/VendorID=5/00003-443-06baebe6-78e0-4b88-a926-77fd7cc61dbd-00003.parquet",
    "s3a://warehouse/wh/nyc/taxis/data/VendorID=5/00003-434-3950631d-6608-4c11-8359-5ef0ee9512af-00003.parquet",
]
