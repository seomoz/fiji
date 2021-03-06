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

package com.moz.fiji.schema.testutil;

import org.junit.After;
import org.junit.Before;

import com.moz.fiji.schema.Fiji;
import com.moz.fiji.schema.FijiTable;
import com.moz.fiji.schema.util.ResourceUtils;

/**
 * An integration test that sets up the Fiji table "foo" before tests.
 */
public class FooTableIntegrationTest extends AbstractFijiIntegrationTest {

  /** Fiji instance used for this test. */
  private Fiji mFiji;

  /** Fiji table instance connected to test table "foo". */
  private FijiTable mFijiTable;

  /**
   * Gets the Fiji instance used by this test.
   *
   * @return The Fiji instance.
   */
  public Fiji getFiji() {
    return mFiji;
  }

  /**
   * Gets the FijiTable instance configured to use the "foo" table.
   *
   * @return The Fiji table.
   */
  public FijiTable getFooTable() {
    return mFijiTable;
  }
  /**
   * Setup the resources needed by this test.
   *
   * @throws Exception If there is an error.
   */
  @Before
  public void setupFooTable() throws Exception {
    createAndPopulateFooTable();
    mFiji = Fiji.Factory.open(getFijiURI(), getIntegrationHelper().getConf());
    mFijiTable = mFiji.openTable("foo");
  }

  /**
   * Releases the resources needed by this test.
   *
   * @throws Exception If there is an error.
   */
  @After
  public void teardownFooTable() throws Exception {
    deleteFooTable();
    ResourceUtils.releaseOrLog(mFijiTable);
    ResourceUtils.releaseOrLog(mFiji);
  }
}
