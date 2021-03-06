/**
 * (c) Copyright 2014 WibiData, Inc.
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

package com.moz.fiji.mapreduce.framework;

import com.moz.fiji.schema.hbase.HBaseFijiURI;


/**
 * Factory for getting instances of FijiTableInputFormat (for tables backed by HBase).
 */
public class HBaseFijiTableInputFormatFactory implements FijiTableInputFormatFactory {
  /** {@inheritDoc} */
  @Override
  public FijiTableInputFormat getInputFormat() {
    return new HBaseFijiTableInputFormat();
  }

  /** {@inheritDoc} */
  @Override
  public String getName() {
    return HBaseFijiURI.HBASE_SCHEME;
  }
}
