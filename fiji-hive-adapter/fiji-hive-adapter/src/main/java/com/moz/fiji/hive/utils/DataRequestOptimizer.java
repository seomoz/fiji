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

import java.util.Collection;
import java.util.List;
import java.util.Map;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.moz.fiji.hive.FijiRowExpression;
import com.moz.fiji.schema.FijiColumnName;
import com.moz.fiji.schema.FijiDataRequest;
import com.moz.fiji.schema.FijiDataRequest.Column;
import com.moz.fiji.schema.FijiDataRequestBuilder;

/**
 * Creates the data request required for the hive query to execute.
 */
public final class DataRequestOptimizer {
  private static final Logger LOG = LoggerFactory.getLogger(DataRequestOptimizer.class);

  /** Utility class cannot be instantiated. */
  private DataRequestOptimizer() {}

  //TODO make this a singleton

  /**
   * Constructs the data request required to read the data in the given expressions.
   *
   * @param expressions The Fiji row expressions describing the data to read.
   * @return The data request.
   */
  public static FijiDataRequest getDataRequest(List<FijiRowExpression> expressions) {
    // TODO: Use only the expressions that are used in the current query.

    // TODO: Don't request all versions at all timestamps if we don't have to.
    FijiDataRequest merged = FijiDataRequest.builder().build();

    //TODO Rewrite this to use new builder semantics.
    for (FijiRowExpression expression : expressions) {
      merged = merged.merge(expression.getDataRequest());
    }

    // If this is a * build an expression that includes everything
    return merged;
  }

  /**
   * Constructs a data request with cell paging enabled for the specified columns.
   *
   * @param fijiDataRequest to use as a base.
   * @param cellPagingMap of fiji columns to page sizes.
   * @return A new data request with paging enabled for the specified columns.
   */
  public static FijiDataRequest addCellPaging(FijiDataRequest fijiDataRequest,
                                              Map<FijiColumnName, Integer> cellPagingMap) {
    FijiDataRequestBuilder pagedRequestBuilder = FijiDataRequest.builder();
    for (Column column : fijiDataRequest.getColumns()) {
      FijiColumnName fijiColumnName = column.getColumnName();
      if (cellPagingMap.containsKey(fijiColumnName)) {
        Integer pageSize = cellPagingMap.get(fijiColumnName);
        pagedRequestBuilder.newColumnsDef().withPageSize(pageSize).add(fijiColumnName);
      }
    }
    FijiDataRequest merged = fijiDataRequest.merge(pagedRequestBuilder.build());
    return merged;
  }

  /**
   * Constructs a data request with paging enabled for the specified family.
   *
   * @param fijiDataRequest to use as a base.
   * @param qualifierPagingMap of fiji columns to page sizes.
   * @return A new data request with paging enabled for the specified family.
   */
  public static FijiDataRequest addQualifierPaging(FijiDataRequest fijiDataRequest,
                                              Map<FijiColumnName, Integer> qualifierPagingMap) {
    FijiDataRequestBuilder pagedRequestBuilder = FijiDataRequest.builder();
    for (Column column : fijiDataRequest.getColumns()) {
      FijiColumnName fijiColumnName = column.getColumnName();
      if (qualifierPagingMap.containsKey(fijiColumnName)) {
        Integer pageSize = qualifierPagingMap.get(fijiColumnName);
        pagedRequestBuilder.newColumnsDef().withPageSize(pageSize).add(fijiColumnName);
      }
    }
    FijiDataRequest merged = fijiDataRequest.merge(pagedRequestBuilder.build());
    return merged;
  }

  /**
   * This method propogates the configuration of a family in a FijiDataRequest by replacing
   * it with a page of fully qualified columns with the same configuration.
   *
   * @param fijiDataRequest to use as a base.
   * @param qualifiersPage a page of fully qualified columns to replace families in the original
   *                        data request with.
   * @return A new data request with the appropriate families replaced with the page of fully
   * qualified columns.
   */
  public static FijiDataRequest expandFamilyWithPagedQualifiers(
      FijiDataRequest fijiDataRequest,
      Collection<FijiColumnName> qualifiersPage) {

    // Organize the page of column names by family.
    Multimap<String, FijiColumnName> familyToQualifiersMap = ArrayListMultimap.create();
    for (FijiColumnName fijiColumnName : qualifiersPage) {
      familyToQualifiersMap.put(fijiColumnName.getFamily(), fijiColumnName);
    }

    // Build a new data request
    FijiDataRequestBuilder qualifierRequestBuilder = FijiDataRequest.builder();
    for (Column column : fijiDataRequest.getColumns()) {
      FijiColumnName fijiColumnName = column.getColumnName();
      if (fijiColumnName.isFullyQualified()
          || !familyToQualifiersMap.containsKey(fijiColumnName.getFamily())) {
        // If the column is fully qualified or it's not in qualifiersPage add this column as is.
        qualifierRequestBuilder.newColumnsDef(column);
      } else {
        // Iterate through the paged qualifiers and add
        for (FijiColumnName columnName : familyToQualifiersMap.get(fijiColumnName.getFamily())) {
          qualifierRequestBuilder.newColumnsDef()
              .withFilter(column.getFilter())
              .withPageSize(column.getPageSize())
              .withMaxVersions(column.getMaxVersions())
              .add(columnName.getFamily(), columnName.getQualifier());
        }
      }
    }
    return qualifierRequestBuilder.build();
  }
}
