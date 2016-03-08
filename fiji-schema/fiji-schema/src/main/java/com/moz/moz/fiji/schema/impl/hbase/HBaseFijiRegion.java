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

package com.moz.fiji.schema.impl.hbase;

import java.util.ArrayList;
import java.util.List;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableList.Builder;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HRegionLocation;

import com.moz.fiji.annotations.ApiAudience;
import com.moz.fiji.schema.FijiRegion;

/**
 * HBaseFijiRegion is the HBase-backed FijiRegion.
 */
@ApiAudience.Private
final class HBaseFijiRegion implements FijiRegion {
  private final HRegionInfo mHRegionInfo;
  private final List<String> mRegionLocations;

  /**
   * Constructs a new HBaseFijiRegion backed by an HRegionInfo.
   *
   * @param hRegionInfo The underlying HRegionInfo.
   * @param locations The HRegionLocations that this region spans.
   */
  HBaseFijiRegion(HRegionInfo hRegionInfo, List<HRegionLocation> locations) {
    mHRegionInfo = hRegionInfo;
    Builder<String> locationsBuilder = ImmutableList.builder();
    for (HRegionLocation hLocation : locations) {
      locationsBuilder.add(hLocation.getHostnamePort());
    }
    mRegionLocations = locationsBuilder.build();
  }

  /**
   * Constructs a new HBaseFijiRegion backed by an HRegionInfo, with no locality information.
   *
   * @param hRegionInfo The underlying HRegionInfo.
   */
  HBaseFijiRegion(HRegionInfo hRegionInfo) {
    this(hRegionInfo, new ArrayList<HRegionLocation>());
  }

  /** {@inheritDoc} */
  @Override
  public byte[] getStartKey() {
    return mHRegionInfo.getStartKey();
  }

  /** {@inheritDoc} */
  @Override
  public byte[] getEndKey() {
    return mHRegionInfo.getEndKey();
  }

  /** {@inheritDoc} */
  @Override
  public List<String> getLocations() {
    return mRegionLocations;
  }

}
