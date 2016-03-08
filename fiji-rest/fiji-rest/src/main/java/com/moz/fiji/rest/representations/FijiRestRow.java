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

package com.moz.fiji.rest.representations;

import java.util.List;
import java.util.NavigableMap;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import com.moz.fiji.schema.FijiCell;

/**
 * Represents the Fiji row returned to the client.
 *
 */
@JsonPropertyOrder({ "entityId", "rowKey" })
public class FijiRestRow {

  @JsonProperty("entityId")
  private FijiRestEntityId mFijiRestEntityId;

  @JsonProperty("cells")
  private NavigableMap<String, NavigableMap<String, List<FijiRestCell>>> mFijiCellMap;

  /**
   * Dummy constructor required for Jackson to (de)serialize JSON properly.
   */
  public FijiRestRow() {
  }

  /**
   * Construct a new FijiRestRow object given the entity id.
   *
   * @param entityId is the entity id of the row.
   */
  public FijiRestRow(FijiRestEntityId entityId) {
    mFijiRestEntityId = entityId;
    mFijiCellMap = Maps.newTreeMap();
  }

  /**
   * Convenience method to add a new cell (represented by the FijiCell) to the row.
   *
   * @param cell is the cell to add.
   * @param schemaOption is the writer schema (contained as an option: either as a string or uid)
   */
  public void addCell(FijiCell<?> cell, SchemaOption schemaOption) {
    addCell(cell.getFamily(), cell.getQualifier(), cell.getTimestamp(), cell.getData(),
        schemaOption);
  }

  /**
   * Adds a new cell (specified by the family, qualifier, timestamp and value) to the current row.
   *
   * @param family is the family of the cell to add.
   * @param qualifier is the qualifier of the cell to add.
   * @param timestamp is the timestamp of the cell to add.
   * @param value is the value of the cell to add.
   * @param writerSchema is the writer schema (contained as an option: either as a string or uid)
   *        of the cell's value.
   */
  public void addCell(String family, String qualifier, Long timestamp, Object value,
      SchemaOption writerSchema) {
    NavigableMap<String, List<FijiRestCell>> familyMap = mFijiCellMap.get(family);
    if (familyMap == null) {
      familyMap = Maps.newTreeMap();
      mFijiCellMap.put(family, familyMap);
    }
    List<FijiRestCell> restCells = familyMap.get(qualifier);
    if (restCells == null) {
      restCells = Lists.newArrayList();
      familyMap.put(qualifier, restCells);
    }
    restCells.add(new FijiRestCell(timestamp, value, writerSchema));
  }

  /**
   * Returns the human readable entity_id (i.e. a string representation of the list of
   * components).
   *
   * @return the human readable entity_id (i.e. a string representation of the list of
   *         components).
   */
  public FijiRestEntityId getEntityId() {
    return mFijiRestEntityId;
  }

  /**
   * Returns the map of cell data contained in this row.
   *
   * @return the map of cell data contained in this row.
   */
  public NavigableMap<String, NavigableMap<String, List<FijiRestCell>>> getCells() {
    return mFijiCellMap;
  }
}
