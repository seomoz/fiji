/**
 * (c) Copyright 2012 WibiData, Inc.
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

package com.moz.fiji.schema;

import com.moz.fiji.annotations.ApiAudience;
import com.moz.fiji.annotations.ApiStability;
import com.moz.fiji.schema.layout.FijiTableLayout;

/**
 * This class validates a {@link FijiDataRequest} against the layout
 * of a Fiji table to make sure it contains all of the columns requested.
 *
 * <p>Application authors cannot instantiate this class directly. Instead they
 * should use the factory method {@link #validatorForLayout(FijiTableLayout)}
 * to get a validator. They can then use its {@link #validate(FijiDataRequest)}
 * method to validate requests.</p>
 */
@ApiAudience.Framework
@ApiStability.Evolving
public final class FijiDataRequestValidator {
  /** The FijiTableLayout to validate against. */
  private final FijiTableLayout mTableLayout;

  /**
   * Construct a validator for a layout.
   *
   * @param tableLayout The table layout to validate against.
   */
  private FijiDataRequestValidator(FijiTableLayout tableLayout) {
    mTableLayout = tableLayout;
  }

  /**
   * Creates a validator for a table layout.
   *
   * @param tableLayout The table layout that requests will be validated against. Cannot be null.
   * @return A validator for the table layout.
   */
  public static FijiDataRequestValidator validatorForLayout(FijiTableLayout tableLayout) {
    if (null == tableLayout) {
      throw new IllegalArgumentException("Cannot create a validator for a null table layout.");
    }
    return new FijiDataRequestValidator(tableLayout);
  }

  /**
   * Validates a data request against this validator's table layout.
   *
   * @param dataRequest The FijiDataRequest to validate.
   * @throws FijiDataRequestException If the data request is invalid.
   */
  public void validate(FijiDataRequest dataRequest) {
    for (FijiDataRequest.Column column : dataRequest.getColumns()) {
      final String qualifier = column.getQualifier();
      final FijiTableLayout.LocalityGroupLayout.FamilyLayout fLayout =
          mTableLayout.getFamilyMap().get(column.getFamily());

      if (null == fLayout) {
        throw new FijiDataRequestException(String.format("Table '%s' has no family named '%s'.",
            mTableLayout.getName(), column.getFamily()));
      }

      if (fLayout.isGroupType() && (null != column.getQualifier())) {
        if (!fLayout.getColumnMap().containsKey(qualifier)) {
          throw new FijiDataRequestException(String.format("Table '%s' has no column '%s'.",
              mTableLayout.getName(), column.getName()));
        }
      }
    }
  }
}
