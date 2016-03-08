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

import static org.junit.Assert.assertEquals;

import org.junit.Before;
import org.junit.Test;

import com.moz.fiji.schema.layout.InvalidLayoutException;
import com.moz.fiji.schema.layout.FijiTableLayout;
import com.moz.fiji.schema.layout.FijiTableLayouts;


public class TestFijiDataRequestValidator extends FijiClientTest {
  private FijiTableLayout mTableLayout;
  private FijiDataRequestValidator mValidator;

  @Before
  public void setupLayout() throws Exception {
    mTableLayout =
        FijiTableLayouts.getTableLayout(FijiTableLayouts.FULL_FEATURED);
    getFiji().createTable(mTableLayout.getDesc());
    mValidator = FijiDataRequestValidator.validatorForLayout(mTableLayout);
  }

  @Test
  public void testValidate() throws InvalidLayoutException {
    FijiDataRequestBuilder builder = FijiDataRequest.builder().withTimeRange(2, 3);
    builder.newColumnsDef().withMaxVersions(1).add("info", "name");
    FijiDataRequest request = builder.build();

    mValidator.validate(request);
  }

  @Test
  public void testValidateNoSuchFamily() throws InvalidLayoutException {
    FijiDataRequestBuilder builder = FijiDataRequest.builder().withTimeRange(2, 3);
    builder.newColumnsDef().withMaxVersions(1).add("blahblah", "name");
    FijiDataRequest request = builder.build();

    try {
      mValidator.validate(request);
    } catch (FijiDataRequestException kdre) {
      assertEquals("Table 'user' has no family named 'blahblah'.", kdre.getMessage());
    }
  }

  @Test
  public void testValidateNoSuchColumn() throws InvalidLayoutException {
    FijiDataRequestBuilder builder = FijiDataRequest.builder().withTimeRange(2, 3);
    builder.newColumnsDef().withMaxVersions(1)
        .add("info", "name")
        .add("info", "blahblah");
    FijiDataRequest request = builder.build();

    try {
      mValidator.validate(request);
    } catch (FijiDataRequestException kdre) {
      assertEquals("Table 'user' has no column 'info:blahblah'.", kdre.getMessage());
    }
  }
}
