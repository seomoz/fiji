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

package com.moz.fiji.schema.platform;

import java.util.Map;

import com.moz.fiji.annotations.ApiAudience;
import com.moz.fiji.delegation.Priority;

/**
 * Factory for CDH5 SchemaPlatformBridge implementation.
 *
 * <p>This is the only CDH5 bridge. Future CDH5 releases will
 * automatically fall back to this bridge.
 *
 * This is also the current bridge for Hadoop 2 and HBase 0.95-0.98.</p>
 */
@ApiAudience.Private
public final class CDH5SchemaBridgeFactory extends SchemaPlatformBridgeFactory {

  /** {@inheritDoc} */
  @Override
  @SuppressWarnings("unchecked")
  public SchemaPlatformBridge getBridge() {
    try {
      Class<? extends SchemaPlatformBridge> bridgeClass =
          (Class<? extends SchemaPlatformBridge>) Class.forName(
              "com.moz.fiji.schema.platform.CDH5SchemaBridge");
      return bridgeClass.newInstance();
    } catch (Exception e) {
      throw new RuntimeException("Could not instantiate platform bridge", e);
    }
  }

  /** {@inheritDoc} */
  @Override
  public int getPriority(Map<String, String> runtimeHints) {
    String hadoopVer = org.apache.hadoop.util.VersionInfo.getVersion();
    String hbaseVer = org.apache.hadoop.hbase.util.VersionInfo.getVersion();

    if (hadoopVer.matches("\\..*-cdh5\\..*") && hbaseVer.matches("\\..*-cdh5\\..*")) {
      // This is the bridge for CDH5.
      return Priority.HIGH;
    } else if (hadoopVer.matches("2\\..*") && hbaseVer.matches("0\\.9[568]\\..*")) {
      // This bridge may work for Hadoop 2 and HBase 0.95/0.96/0.98.
      return Priority.NORMAL;
    } else {
      // Can't provide for this implementation.
      return Priority.DISABLED;
    }
  }
}

