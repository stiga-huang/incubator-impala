// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package org.apache.impala.authorization;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import org.apache.impala.analysis.Expr;
import org.apache.impala.analysis.Parser;
import org.apache.impala.analysis.SelectStmt;
import org.apache.impala.analysis.SlotRef;
import org.apache.impala.catalog.FeTable;
import org.apache.impala.catalog.Type;
import org.apache.impala.common.AnalysisException;
import org.apache.impala.common.InternalException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * A TableMask instance contains all the information for generating a subquery for table
 * masking (column masking / row filtering).
 */
public class TableMask {
  private static final Logger LOG = LoggerFactory.getLogger(TableMask.class);

  private AuthorizationChecker authChecker_;
  private String dbName_;
  private String tableName_;
  private List<String> requiredColumns_;
  private User user_;

  public TableMask(AuthorizationChecker authzChecker, FeTable table, User user) {
    this.authChecker_ = authzChecker;
    this.dbName_ = table.getDb().getName();
    this.tableName_ = table.getName();
    // TODO: only require materialize columns to avoid unneccessary masking so we won't
    //       hit IMPALA-9223
    this.requiredColumns_ = table.getColumnNames();
    this.user_ = user;
  }

  /**
   * Returns whether the table/view has column masking or row filtering policies.
   */
  public boolean needsMaskingOrFiltering() throws InternalException {
    return authChecker_.needsMaskingOrFiltering(user_, dbName_, tableName_,
        requiredColumns_);
  }

  /**
   * Returns whether the table/view has row filtering policies.
   */
  public boolean needsRowFiltering() throws InternalException {
    return authChecker_.needsRowFiltering(user_, dbName_, tableName_);
  }

  public SelectStmt createColumnMaskStmt(String colName, Type colType,
      AuthorizationContext authzCtx) throws InternalException,
      AnalysisException {
    Preconditions.checkState(!colType.isComplexType());
    String maskedValue = authChecker_.createColumnMask(user_, dbName_, tableName_,
        colName, authzCtx);
    if (LOG.isTraceEnabled()) {
      LOG.trace("Performing column masking on table {}.{}: {} => {}",
          dbName_, tableName_, colName, maskedValue);
    }
    if (maskedValue == null || maskedValue.equals(colName)) {  // Don't need masking.
      return null;
    }
    SelectStmt maskStmt = (SelectStmt) Parser.parse(
        String.format("SELECT CAST(%s AS %s)", maskedValue, colType));
    if (maskStmt.getSelectList().getItems().size() != 1 || maskStmt.hasGroupByClause()
        || maskStmt.hasHavingClause() || maskStmt.hasWhereClause()) {
      throw new AnalysisException("Illegal column masked value: " + maskedValue);
    }
    return maskStmt;
  }

  /**
   * Return the masked Expr of the given column
   */
  public Expr createColumnMask(String colName, Type colType,
      AuthorizationContext authzCtx) throws InternalException,
      AnalysisException {
    SelectStmt maskStmt = createColumnMaskStmt(colName, colType, authzCtx);
    if (maskStmt == null) return new SlotRef(Lists.newArrayList(colName));
    Expr res = maskStmt.getSelectList().getItems().get(0).getExpr();
    if (LOG.isTraceEnabled()) {
      LOG.trace("Returned Expr: " + res.toSql());
    }
    return res;
  }

  public SelectStmt createRowFilterStmt(AuthorizationContext authzCtx)
      throws InternalException, AnalysisException {
    String rowFilter = authChecker_.createRowFilter(user_, dbName_, tableName_, authzCtx);
    if (rowFilter == null) return null;
    String stmtSql = String.format("SELECT * FROM %s.%s WHERE %s",
        dbName_, tableName_, rowFilter);
    return (SelectStmt) Parser.parse(stmtSql);
  }

  /**
   * Return the row filter Expr
   */
  public Expr createRowFilter(AuthorizationContext authzCtx)
      throws InternalException, AnalysisException {
    SelectStmt selectStmt = createRowFilterStmt(authzCtx);
    if (selectStmt == null) return null;
    Expr wherePredicate = selectStmt.getWhereClause();
    // No recursive masking, i.e. table refs introduced in subquery row-filters won't be
    // masked. This is consistent to Hive. The other reason is to avoid infinitely masking
    // when the subqeury filter use the same table.
    wherePredicate.setDoTableMasking(false);
    return wherePredicate;
  }
}
