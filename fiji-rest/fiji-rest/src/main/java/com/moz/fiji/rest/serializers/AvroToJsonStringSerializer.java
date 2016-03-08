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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.Charset;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializerProvider;

import org.apache.avro.AvroRuntimeException;
import org.apache.avro.generic.GenericContainer;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.io.JsonEncoder;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.avro.specific.SpecificRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Avro's specific types don't seem to serialize properly so this class provides
 * the necessary hook to convert an Avro SpecificRecordBase to a Json string literal meant to
 * be embedded in another JSON object sent back to the client.
 *
 */
public class AvroToJsonStringSerializer extends JsonSerializer<GenericContainer> {

  private static final Logger LOG = LoggerFactory.getLogger(AvroToJsonStringSerializer.class);

  /**
   * {@inheritDoc}
   */
  @Override
  public void serialize(GenericContainer record, JsonGenerator generator,
      SerializerProvider provider) throws IOException {
    try {
      generator.writeObject(getJsonNode(record));
    } catch (AvroRuntimeException are) {
      LOG.error("Error writing Avro record ", are);
      throw are;
    }
  }

  /**
   * Returns an encoded JSON string for the given Avro object.
   *
   * @param record is the record to encode
   * @return the JSON string representing this Avro object.
   *
   * @throws IOException if there is an error.
   */
  public static String getJsonString(GenericContainer record) throws IOException {
    ByteArrayOutputStream os = new ByteArrayOutputStream();
    JsonEncoder encoder = EncoderFactory.get().jsonEncoder(record.getSchema(), os);
    DatumWriter<GenericContainer> writer = new GenericDatumWriter<GenericContainer>();
    if (record instanceof SpecificRecord) {
      writer = new SpecificDatumWriter<GenericContainer>();
    }

    writer.setSchema(record.getSchema());
    writer.write(record, encoder);
    encoder.flush();
    String jsonString = new String(os.toByteArray(), Charset.forName("UTF-8"));
    os.close();
    return jsonString;
  }

  /**
   * Returns an encoded JSON object for the given Avro object.
   *
   * @param record is the record to encode
   * @return the JSON object representing this Avro object.
   *
   * @throws IOException if there is an error.
   */
  public static JsonNode getJsonNode(GenericContainer record) throws IOException {
    ObjectMapper mapper = new ObjectMapper();
    return mapper.readTree(getJsonString(record));
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Class<GenericContainer> handledType() {
    return GenericContainer.class;
  }
}
