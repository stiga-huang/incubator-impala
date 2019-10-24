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

from tests.common.custom_cluster_test_suite import CustomClusterTestSuite

import pytest
from os import getenv

HIVE_SITE_EXT_DIR = getenv('IMPALA_HOME') + '/fe/src/test/resources/hive-site-ext'


class TestCustomHiveConfigs(CustomClusterTestSuite):
  @classmethod
  def get_workload(cls):
    return 'functional-query'

  @classmethod
  def setup_class(cls):
    super(TestCustomHiveConfigs, cls).setup_class()

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(hive_conf_dir=HIVE_SITE_EXT_DIR)
  def test_ctas_read_write_consistence(self, unique_database):
    """
    IMPALA-9071: Check CTAS inserts data to the correct directory when
    'metastore.warehouse.external.dir' is different with 'metastore.warehouse.dir'
    in Hive.
    """
    try:
      self.execute_query_expect_success(
        self.client, 'create database if not exists ' + unique_database)
      self.execute_query_expect_success(
        self.client, 'create table %s.ctas_tbl as select 1, 2, "name"' % unique_database)
      res = self.execute_query_expect_success(
        self.client, 'select * from %s.ctas_tbl' % unique_database)
      assert '1\t2\tname' == res.get_data()
    finally:
      self.execute_query_expect_success(
        self.client, 'drop database if exists cascade' + unique_database)
