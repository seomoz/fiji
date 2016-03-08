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

package com.moz.fiji.mapreduce.util;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import org.junit.Before;
import org.junit.Test;

import com.moz.fiji.mapreduce.produce.FijiProducer;
import com.moz.fiji.mapreduce.produce.FijiProducerOutputException;
import com.moz.fiji.mapreduce.produce.ProducerContext;
import com.moz.fiji.mapreduce.produce.impl.FijiProducers;
import com.moz.fiji.schema.FijiClientTest;
import com.moz.fiji.schema.FijiDataRequest;
import com.moz.fiji.schema.FijiRowData;
import com.moz.fiji.schema.layout.InvalidLayoutException;
import com.moz.fiji.schema.layout.FijiTableLayout;
import com.moz.fiji.schema.layout.FijiTableLayouts;

public class TestFijiProducers extends FijiClientTest {
  private FijiTableLayout mTableLayout;

  @Before
  public void setupLayout() throws Exception {
    mTableLayout = FijiTableLayouts.getTableLayout(FijiTableLayouts.SIMPLE);
    getFiji().createTable(mTableLayout.getName(), mTableLayout);
  }

  public static class MyProducer extends FijiProducer {
    @Override
    public FijiDataRequest getDataRequest() {
      return null;
    }

    @Override
    public String getOutputColumn() {
      return "family:column";
    }

    @Override
    public void produce(FijiRowData input, ProducerContext context) {
      // meh.
    }
  }

  @Test
  public void testValidateOutputColumn()
      throws InvalidLayoutException, FijiProducerOutputException {
    MyProducer producer = new MyProducer();
    FijiProducers.validateOutputColumn(producer, mTableLayout);
  }

  @Test
  public void testValidateOutputColumnNonExistentFamily()
      throws InvalidLayoutException, FijiProducerOutputException {
    MyProducer producer = new MyProducer() {
      @Override
      public String getOutputColumn() {
        return "doesnt_exist:column";
      }
    };

    try {
      FijiProducers.validateOutputColumn(producer, mTableLayout);
      fail("Should have gotten a FijiProducerOutputException.");
    } catch (FijiProducerOutputException ke) {
      assertEquals("Producer 'com.moz.fiji.mapreduce.util.TestFijiProducers$1' specifies "
          + "unknown output column family 'doesnt_exist' in table 'table'.",
          ke.getMessage());
    }
  }

  @Test
  public void testValidateOutputColumnNonExistentColumn()
      throws InvalidLayoutException, FijiProducerOutputException {
    MyProducer producer = new MyProducer() {
      @Override
      public String getOutputColumn() {
        return "family:doesnt_exist";
      }
    };

    try {
      FijiProducers.validateOutputColumn(producer, mTableLayout);
      fail("Should have gotten a FijiProducerOutputException.");
    } catch (FijiProducerOutputException ke) {
      assertEquals("Producer 'com.moz.fiji.mapreduce.util.TestFijiProducers$2' specifies "
          + "unknown column 'family:doesnt_exist' in table 'table'.",
          ke.getMessage());
    }
 }

  /**
   * Not specifying a qualifier even to a group type is allowed so that you can write to
   * multiple groups in a column.
   */
  @Test
  public void testValidateOutputColumnNoQualifierToGroupType()
      throws InvalidLayoutException, FijiProducerOutputException {
    MyProducer producer = new MyProducer() {
      @Override
      public String getOutputColumn() {
        return "family";
      }
    };
    // This should validate just fine.
    FijiProducers.validateOutputColumn(producer, mTableLayout);
  }
}
