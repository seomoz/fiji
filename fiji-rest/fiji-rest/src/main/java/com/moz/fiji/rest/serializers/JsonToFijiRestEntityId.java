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

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import com.moz.fiji.rest.representations.FijiRestEntityId;

/**
 * Converts a JSON object to a SchemaOption.
 *
 */
public class JsonToFijiRestEntityId extends JsonDeserializer<FijiRestEntityId> {

  /**
   * {@inheritDoc}
   */
  @Override
  public FijiRestEntityId deserialize(JsonParser parser, DeserializationContext context)
      throws IOException {

    ObjectMapper mapper = new ObjectMapper();
    JsonNode node = mapper.readTree(parser);
    if (node.isArray()) {
      return FijiRestEntityId.create(node);
    } else if (node.isTextual()) {
      return FijiRestEntityId.create(node.asText());
    } else {
      throw new IllegalArgumentException(
          "Entity id node to deserialize must be an array or textual, it was: " + node);
    }
  }

}
