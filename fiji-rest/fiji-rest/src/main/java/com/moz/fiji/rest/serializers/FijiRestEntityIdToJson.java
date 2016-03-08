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

package com.moz.fiji.rest.serializers;

import java.io.IOException;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.moz.fiji.rest.representations.FijiRestEntityId;

/**
 * Converts the SchemaOption to JSON.
 *
 */
public class FijiRestEntityIdToJson extends JsonSerializer<FijiRestEntityId> {

  private static final Logger LOG = LoggerFactory.getLogger(FijiRestEntityIdToJson.class);

  /**
   * {@inheritDoc}
   */
  @Override
  public void serialize(FijiRestEntityId record, JsonGenerator generator,
      SerializerProvider provider) throws IOException {
    if (record.hasComponents()) {
      generator.writeTree(record.getJsonEntityId());
    } else {
      generator.writeString(record.getStringEntityId());
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Class<FijiRestEntityId> handledType() {
    return FijiRestEntityId.class;
  }
}
