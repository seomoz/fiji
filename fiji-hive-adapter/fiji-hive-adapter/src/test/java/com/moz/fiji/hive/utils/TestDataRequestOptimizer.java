/**
 * (c) Copyright 2013 WibiData, Inc.
 *
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.moz.fiji.hive.utils;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.hadoop.hbase.HConstants;
import org.junit.Test;

import com.moz.fiji.hive.FijiRowExpression;
import com.moz.fiji.hive.TypeInfos;
import com.moz.fiji.schema.FijiColumnName;
import com.moz.fiji.schema.FijiDataRequest;

public class TestDataRequestOptimizer {
  @Test
  public void testEmpty() throws IOException {
    List<FijiRowExpression> rowExpressionList = Lists.newArrayList();
    FijiDataRequest fijiDataRequest = DataRequestOptimizer.getDataRequest(rowExpressionList);
    assertTrue(fijiDataRequest.isEmpty());
  }

  @Test
  public void testSingleExpression() throws IOException {
    final FijiRowExpression fijiRowExpression =
        new FijiRowExpression("info:name", TypeInfos.COLUMN_ALL_VALUES);
    List<FijiRowExpression> rowExpressionList = Lists.newArrayList(fijiRowExpression);

    FijiDataRequest fijiDataRequest = DataRequestOptimizer.getDataRequest(rowExpressionList);
    assertEquals(1, fijiDataRequest.getColumns().size());
    assertNotNull(fijiDataRequest.getColumn("info", "name"));
    assertNull(fijiDataRequest.getColumn("info", "address"));
  }

  @Test
  public void testMergeMultipleExpressions() throws IOException {
    final FijiRowExpression nameRowExpression =
        new FijiRowExpression("info:email", TypeInfos.COLUMN_ALL_VALUES);
    final FijiRowExpression emailRowExpression =
        new FijiRowExpression("info:name", TypeInfos.COLUMN_ALL_VALUES);
    List<FijiRowExpression> rowExpressionList =
        Lists.newArrayList(nameRowExpression, emailRowExpression);

    FijiDataRequest fijiDataRequest = DataRequestOptimizer.getDataRequest(rowExpressionList);
    assertEquals(2, fijiDataRequest.getColumns().size());
    assertNotNull(fijiDataRequest.getColumn("info", "name"));
    assertNotNull(fijiDataRequest.getColumn("info", "email"));
    assertNull(fijiDataRequest.getColumn("info", "address"));
  }

  @Test
  public void testMergeVersions() throws IOException {
    // Test that an arbitrary index summed with all versions results in all versions.
    final FijiRowExpression allEmailsRowExpression =
        new FijiRowExpression("info:email", TypeInfos.COLUMN_ALL_VALUES);
    final FijiRowExpression newestEmailExpression =
        new FijiRowExpression("info:email[0]", TypeInfos.COLUMN_FLAT_VALUE);
    List<FijiRowExpression> rowExpressionList =
        Lists.newArrayList(allEmailsRowExpression, newestEmailExpression);

    FijiDataRequest fijiDataRequest = DataRequestOptimizer.getDataRequest(rowExpressionList);
    assertEquals(1, fijiDataRequest.getColumns().size());
    for (FijiDataRequest.Column column : fijiDataRequest.getColumns()) {
      assertEquals(HConstants.ALL_VERSIONS,
          fijiDataRequest.getColumn(column.getFamily(), column.getQualifier()).getMaxVersions());
    }

    // Test that an arbitrary values added togather results in the larger value
    final FijiRowExpression secondNewestEmailExpression =
        new FijiRowExpression("info:email[3]", TypeInfos.COLUMN_FLAT_VALUE);
    rowExpressionList = Lists.newArrayList(newestEmailExpression, secondNewestEmailExpression);

    fijiDataRequest = DataRequestOptimizer.getDataRequest(rowExpressionList);
    assertEquals(1, fijiDataRequest.getColumns().size());
    for (FijiDataRequest.Column column : fijiDataRequest.getColumns()) {
      assertEquals(4,
          fijiDataRequest.getColumn(column.getFamily(), column.getQualifier()).getMaxVersions());
    }
  }

  @Test
  public void testMergeFamilyWithoutQualifier() throws IOException {
    // Ensure that an expression that contains just the family will supersede one that contains
    // both a family and a qualifier
    final FijiRowExpression nameRowExpression =
        new FijiRowExpression("info:email", TypeInfos.COLUMN_ALL_VALUES);

    final FijiRowExpression fullInfoRowExpression =
        new FijiRowExpression("info", TypeInfos.FAMILY_MAP_ALL_VALUES);
    List<FijiRowExpression> rowExpressionList =
        Lists.newArrayList(nameRowExpression, fullInfoRowExpression);

    FijiDataRequest fijiDataRequest = DataRequestOptimizer.getDataRequest(rowExpressionList);
    assertEquals(1, fijiDataRequest.getColumns().size());
    for (FijiDataRequest.Column column : fijiDataRequest.getColumns()) {
      assertEquals("info", column.getFamily());
      assertNull(column.getQualifier());
      assertEquals(HConstants.ALL_VERSIONS,
          fijiDataRequest.getColumn(column.getFamily(), column.getQualifier()).getMaxVersions());
    }
  }

  @Test
  public void testAddCellPaging() throws IOException {
    final FijiColumnName fijiColumnName = new FijiColumnName("info:name");
    final FijiRowExpression fijiRowExpression =
        new FijiRowExpression(fijiColumnName.toString(), TypeInfos.COLUMN_ALL_VALUES);
    List<FijiRowExpression> rowExpressionList = Lists.newArrayList(fijiRowExpression);
    final FijiDataRequest baseFijiDataRequest =
        DataRequestOptimizer.getDataRequest(rowExpressionList);
    assertEquals(false, baseFijiDataRequest.isPagingEnabled());

    Map<FijiColumnName, Integer> cellPagingMap = Maps.newHashMap();
    cellPagingMap.put(fijiColumnName, 5);
    final FijiDataRequest pagedDataRequest =
        DataRequestOptimizer.addCellPaging(baseFijiDataRequest, cellPagingMap);
    assertEquals(true, pagedDataRequest.isPagingEnabled());
    assertEquals(baseFijiDataRequest.getColumns().size(), pagedDataRequest.getColumns().size());
  }

  @Test
  public void testNoCellPagingForNonincludedColumns() throws IOException {
    final FijiColumnName fijiColumnName = new FijiColumnName("info:name");
    final FijiRowExpression fijiRowExpression =
        new FijiRowExpression(fijiColumnName.toString(), TypeInfos.COLUMN_ALL_VALUES);
    List<FijiRowExpression> rowExpressionList = Lists.newArrayList(fijiRowExpression);
    final FijiDataRequest baseFijiDataRequest =
        DataRequestOptimizer.getDataRequest(rowExpressionList);
    assertEquals(false, baseFijiDataRequest.isPagingEnabled());

    Map<FijiColumnName, Integer> cellPagingMap = Maps.newHashMap();
    FijiColumnName nonIncludedColumn = new FijiColumnName("info:notincluded");
    cellPagingMap.put(nonIncludedColumn, 5);
    FijiDataRequest pagedDataRequest =
        DataRequestOptimizer.addCellPaging(baseFijiDataRequest, cellPagingMap);

    assertEquals(false, pagedDataRequest.isPagingEnabled());
    assertEquals(baseFijiDataRequest.getColumns().size(), pagedDataRequest.getColumns().size());
  }

  @Test
  public void testNoCellPagingWithZeroPageSize() throws IOException {
    final FijiColumnName fijiColumnName = new FijiColumnName("info:name");
    final FijiRowExpression fijiRowExpression =
        new FijiRowExpression(fijiColumnName.toString(), TypeInfos.COLUMN_ALL_VALUES);
    List<FijiRowExpression> rowExpressionList = Lists.newArrayList(fijiRowExpression);
    final FijiDataRequest baseFijiDataRequest = DataRequestOptimizer
        .getDataRequest(rowExpressionList);
    assertEquals(false, baseFijiDataRequest.isPagingEnabled());

    Map<FijiColumnName, Integer> cellPagingMap = Maps.newHashMap();
    cellPagingMap.put(fijiColumnName, 0);
    final FijiDataRequest pagedDataRequest =
        DataRequestOptimizer.addCellPaging(baseFijiDataRequest, cellPagingMap);
    assertEquals(baseFijiDataRequest.getColumns().size(), pagedDataRequest.getColumns().size());
    assertEquals(false, pagedDataRequest.isPagingEnabled());
  }

  @Test
  public void testAddQualifierPaging() throws IOException {
    final FijiColumnName fijiColumnName = new FijiColumnName("info");
    final FijiRowExpression fijiRowExpression =
        new FijiRowExpression(fijiColumnName.toString(), TypeInfos.FAMILY_MAP_ALL_VALUES);
    List<FijiRowExpression> rowExpressionList = Lists.newArrayList(fijiRowExpression);
    final FijiDataRequest baseFijiDataRequest =
        DataRequestOptimizer.getDataRequest(rowExpressionList);
    assertEquals(false, baseFijiDataRequest.isPagingEnabled());

    Map<FijiColumnName, Integer> qualifierPagingMap = Maps.newHashMap();
    qualifierPagingMap.put(fijiColumnName, 5);
    final FijiDataRequest pagedDataRequest =
        DataRequestOptimizer.addCellPaging(baseFijiDataRequest, qualifierPagingMap);
    assertEquals(true, pagedDataRequest.isPagingEnabled());
    assertEquals(baseFijiDataRequest.getColumns().size(), pagedDataRequest.getColumns().size());
  }

  @Test
  public void testNoQualifierPagingForNonincludedColumns() throws IOException {
    final FijiColumnName fijiColumnName = new FijiColumnName("info");
    final FijiRowExpression fijiRowExpression =
        new FijiRowExpression(fijiColumnName.toString(), TypeInfos.FAMILY_MAP_ALL_VALUES);
    List<FijiRowExpression> rowExpressionList = Lists.newArrayList(fijiRowExpression);
    final FijiDataRequest baseFijiDataRequest =
        DataRequestOptimizer.getDataRequest(rowExpressionList);
    assertEquals(false, baseFijiDataRequest.isPagingEnabled());

    Map<FijiColumnName, Integer> cellPagingMap = Maps.newHashMap();
    FijiColumnName nonIncludedColumn = new FijiColumnName("info:notincluded");
    cellPagingMap.put(nonIncludedColumn, 5);
    FijiDataRequest pagedDataRequest =
        DataRequestOptimizer.addCellPaging(baseFijiDataRequest, cellPagingMap);

    assertEquals(false, pagedDataRequest.isPagingEnabled());
    assertEquals(baseFijiDataRequest.getColumns().size(), pagedDataRequest.getColumns().size());
  }

  @Test
  public void testNoQualifierPagingWithZeroPageSize() throws IOException {
    final FijiColumnName fijiColumnName = new FijiColumnName("info");
    final FijiRowExpression fijiRowExpression =
        new FijiRowExpression(fijiColumnName.toString(), TypeInfos.FAMILY_MAP_ALL_VALUES);
    List<FijiRowExpression> rowExpressionList = Lists.newArrayList(fijiRowExpression);
    final FijiDataRequest baseFijiDataRequest = DataRequestOptimizer
        .getDataRequest(rowExpressionList);
    assertEquals(false, baseFijiDataRequest.isPagingEnabled());

    Map<FijiColumnName, Integer> cellPagingMap = Maps.newHashMap();
    cellPagingMap.put(fijiColumnName, 0);
    final FijiDataRequest pagedDataRequest =
        DataRequestOptimizer.addCellPaging(baseFijiDataRequest, cellPagingMap);
    assertEquals(baseFijiDataRequest.getColumns().size(), pagedDataRequest.getColumns().size());
    assertEquals(false, pagedDataRequest.isPagingEnabled());
  }

  @Test
  public void testSpecifyQualifierPaging() throws IOException {
    final FijiColumnName fijiColumnName = new FijiColumnName("info");
    final FijiRowExpression fijiRowExpression =
        new FijiRowExpression(fijiColumnName.toString(), TypeInfos.FAMILY_MAP_ALL_VALUES);
    List<FijiRowExpression> rowExpressionList = Lists.newArrayList(fijiRowExpression);
    final FijiDataRequest baseFijiDataRequest =
        DataRequestOptimizer.getDataRequest(rowExpressionList);

    Map<FijiColumnName, Integer> qualifierPagingMap = Maps.newHashMap();
    qualifierPagingMap.put(fijiColumnName, 5);

    Map<FijiColumnName, Integer> cellPagingMap = Maps.newHashMap();
    cellPagingMap.put(fijiColumnName, 2);
    final FijiDataRequest pagedDataRequest =
        addQualifierAndCellPaging(baseFijiDataRequest, qualifierPagingMap, cellPagingMap);

    List<FijiColumnName> pagedColumns = Lists.newArrayList();
    pagedColumns.add(new FijiColumnName("info:foo"));
    pagedColumns.add(new FijiColumnName("info:bar"));
    pagedColumns.add(new FijiColumnName("info:baz"));

    final FijiDataRequest specifiedPagedDataRequest =
        DataRequestOptimizer.expandFamilyWithPagedQualifiers(pagedDataRequest, pagedColumns);
    assertEquals(true, pagedDataRequest.isPagingEnabled());

    // Should be 3 columns, since we are replacing the info family with the 3 fully qualified ones.
    assertEquals(3, specifiedPagedDataRequest.getColumns().size());
  }

  /**
   * Convenience method that combines the effects of:
   * {@link com.moz.fiji.hive.utils.DataRequestOptimizer#addQualifierPaging} and
   * {@link com.moz.fiji.hive.utils.DataRequestOptimizer#addCellPaging} for creating a data request
   * with both qualifier and cell paging at once.
   *
   * @param fijiDataRequest to use as a base.
   * @param qualifierPagingMap of fiji columns to page sizes.
   * @param cellPagingMap of fiji columns to page sizes.
   * @return A new data request with paging enabled for the specified family.
   */
  private static FijiDataRequest addQualifierAndCellPaging(
      FijiDataRequest fijiDataRequest,
      Map<FijiColumnName, Integer> qualifierPagingMap,
      Map<FijiColumnName, Integer> cellPagingMap) {
    final FijiDataRequest qualifierPagedDataRequest =
        DataRequestOptimizer.addQualifierPaging(fijiDataRequest, qualifierPagingMap);
    final FijiDataRequest qualifierAndCellPagedDataRequest =
        DataRequestOptimizer.addCellPaging(qualifierPagedDataRequest, cellPagingMap);
    return qualifierAndCellPagedDataRequest;
  }
}
