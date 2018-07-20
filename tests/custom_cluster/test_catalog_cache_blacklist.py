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
#
# Test Catalog Caching Blacklist
# --catalog-cache-blacklist=database1.table1,database2.table2

import logging
import pytest

from tests.common.custom_cluster_test_suite import CustomClusterTestSuite

LOG = logging.getLogger('catalog_cache_blacklist')

class TestCatalogCacheBlacklist(CustomClusterTestSuite):
  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(
      catalogd_args="--catalog-cache-blacklist=tpch.region,tpch.lineitem"
  )
  def test_catalog_cache_blacklist(self):
      """Start Impala cluster with catalog blacklist."""

      try:
        client = self.create_impala_client()
        result = self.execute_query_expect_failure(client, "select count(*) from tpch.region")
        LOG.log(logging.INFO, str(result))
        assert "Table tpch.region is banned" in str(result)
        result = self.execute_query_expect_failure(client, "describe tpch.lineitem")
        LOG.log(logging.INFO, str(result))
        assert "Table tpch.lineitem is banned" in str(result)
        self.execute_query_expect_success(client, "select count(*) from tpch.customer")
        self.execute_query_expect_success(client, "describe tpch.nation")
      finally:
        client.close()
