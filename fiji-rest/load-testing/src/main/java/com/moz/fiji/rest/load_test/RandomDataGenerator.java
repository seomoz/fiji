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

package com.moz.fiji.rest.load_test;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.NavigableMap;
import java.util.Random;

import com.google.common.collect.Maps;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.moz.fiji.schema.EntityId;
import com.moz.fiji.schema.Fiji;
import com.moz.fiji.schema.FijiBufferedWriter;
import com.moz.fiji.schema.FijiTable;
import com.moz.fiji.schema.FijiURI;
import com.moz.fiji.schema.layout.FijiTableLayout;

/**
 * Generates 1 million random user records. Populates the users table whose
 * layout is defined by the inputs/load_test_layout.json.
 * <ul>
 * <li>The first and last names are randomly generated (via weighted probabilities) through data
 * obtained from the US Census (inputs/first_names.txt and inputs/last_names.txt).
 *
 * <li>Email addresses are just faked to be the first_name.last_name@email.com and any resemblance
 * to a real email address is purely coincidental.
 *
 * <li>The clicks column is populated by putting 3 random product_ids in the column.
 * </ul>
 */
public class RandomDataGenerator {

  private static final String TOTAL = "total";
  private static final String FIRST_NAME = "com.moz.fiji/rest/load_test/first_names.txt";
  private static final String LAST_NAME = "com.moz.fiji/rest/load_test/last_names.txt";
  private static final String USER_TABLE_NAME = "users";
  private static final String USER_TABLE_LAYOUT = "/com.moz.fiji/rest/load_test/load_test_layout.json";
  private static final String DEFAULT_INSTANCE = "fiji://.env/default";

  private static final String COMMENT = "#";

  private Random mRandomNumberGenerator = new Random(System.currentTimeMillis());
  private NavigableMap<Integer, String> mLastNames = Maps.newTreeMap();
  private NavigableMap<Integer, String> mMaleFirstNames = Maps.newTreeMap();
  private NavigableMap<Integer, String> mFemaleFirstNames = Maps.newTreeMap();

  private int mNumberOfLastNames = 0;
  private int mNumberOfMaleNames = 0;
  private int mNumberOfFemaleNames = 0;

  private static final int NUMBER_OF_PRODUCTS = 1000000;
  private static final int USER_ID_MAX = 1000000;

  private static final Logger LOG = LoggerFactory.getLogger(RandomDataGenerator.class);

  /**
   * Constructs a new instance.
   *
   * @throws IOException if there is an error reading any of the input files.
   */
  public RandomDataGenerator() throws IOException {
    // Read the files
    BufferedReader firstNameReader = getReader(FIRST_NAME);
    String line = firstNameReader.readLine();
    while (line != null) {
      String[] parts = line.split(" ");
      if (parts.length == 4 && !line.startsWith(COMMENT)) {
        if (parts[0].equalsIgnoreCase(TOTAL)) {
          mNumberOfMaleNames = Integer.parseInt(parts[1]);
          mNumberOfFemaleNames = Integer.parseInt(parts[3]);
        } else {
          // Male first, Female second
          mMaleFirstNames.put(Integer.parseInt(parts[1]) - 1, parts[0]);
          mFemaleFirstNames.put(Integer.parseInt(parts[3]) - 1, parts[2]);
        }
      }
      line = firstNameReader.readLine();
    }
    firstNameReader.close();

    BufferedReader lastNameReader = getReader(LAST_NAME);
    line = lastNameReader.readLine();
    while (line != null) {
      String[] parts = line.split(" ");
      if (parts.length == 2 && !line.startsWith(COMMENT)) {
        if (parts[0].equalsIgnoreCase(TOTAL)) {
          mNumberOfLastNames = Integer.parseInt(parts[1]);
        } else {
          mLastNames.put(Integer.parseInt(parts[1]) - 1, parts[0]);
        }
      }
      line = lastNameReader.readLine();
    }
    lastNameReader.close();
  }

  /**
   * Returns a BufferedReader given a resource string that should be on the classpath.
   *
   * @param resource is the location of the resource to load.
   * @return A BufferedReader wrapping the resource passed in.
   * @throws IOException if resource doesn't exist.
   */
  private static BufferedReader getReader(String resource) throws IOException {
    InputStream is = Thread.currentThread().getContextClassLoader().getResourceAsStream(resource);
    if (is != null) {
      return new BufferedReader(new InputStreamReader(is));
    } else {
      throw new IOException(resource + " is not found!");
    }
  }

  /**
   * Generates the given number of random records and populates the given FijiTable with those
   * random records.
   *
   * @param numberOfRecords is the number of records to generate.
   * @param table is the table to write the data into.
   * @throws IOException If there is an exception writing to the FijiTable.
   */
  public void generateData(long numberOfRecords, FijiTable table) throws IOException {
    String firstName = null, lastName = null, email = null;
    int userId = 0;
    int productId = 0;
    float genderProb = 0.0f;
    NavigableMap<Integer, String> firstNameMap = null;
    int numberOfFirstNames = 0;

    FijiBufferedWriter writer = table.getWriterFactory().openBufferedWriter();
    writer.setBufferSize(10000);

    for (int i = 0; i < numberOfRecords; i++) {
      genderProb = mRandomNumberGenerator.nextFloat();
      // Male or Female
      if (genderProb < 0.50) {
        firstNameMap = mMaleFirstNames;
        numberOfFirstNames = mNumberOfMaleNames;
      } else {
        firstNameMap = mFemaleFirstNames;
        numberOfFirstNames = mNumberOfFemaleNames;
      }
      // Given a random number from [0-numberOfFirstNames), use it's ceiling to
      // get the actual name.
      int firstNameRand = mRandomNumberGenerator.nextInt(numberOfFirstNames);
      firstName = firstNameMap.ceilingEntry(firstNameRand).getValue();
      int lastNameRand = mRandomNumberGenerator.nextInt(mNumberOfLastNames);
      lastName = mLastNames.ceilingEntry(lastNameRand).getValue();
      email = firstName.toLowerCase() + "." + lastName.toLowerCase() + "@email.com";

      userId = i;

      EntityId eid = table.getEntityId(userId);
      writer.put(eid, "info", "name", firstName + " " + lastName);
      writer.put(eid, "info", "email", email);
      writer.put(eid, "info", "id", userId);

      // Generate 3 fake clicks.
      long currentTime = System.currentTimeMillis();
      for (int j = 0; j < 3; j++) {
        writer.put(eid, "info", "clicks", currentTime + (j * 100), productId);
        productId = (int) (mRandomNumberGenerator.nextFloat() * NUMBER_OF_PRODUCTS) + 1;
      }

      if (i % 10000 == 0) {
        LOG.info("Wrote " + i + " rows.");
      }
    }
    writer.close();
  }

  /**
   * Main. Pass in the instance to create the users table in.
   *
   * @param args instance to create user table in.
   * @throws Exception if there is an exception.
   */
  public static void main(String[] args) throws Exception {
    final String userTableInstanceURI;
    if (args.length == 0) {
      userTableInstanceURI = DEFAULT_INSTANCE;
    } else {
      userTableInstanceURI = args[0];
    }
    final FijiURI fijiURI = FijiURI.newBuilder(userTableInstanceURI).build();
    final Fiji fiji = Fiji.Factory.open(fijiURI);

    // Create table.
    fiji.createTable(
        FijiTableLayout
            .createFromEffectiveJsonResource(USER_TABLE_LAYOUT)
            .getDesc());
    final FijiTable userTable = fiji.openTable(USER_TABLE_NAME);

    // Generate data.
    final RandomDataGenerator gen = new RandomDataGenerator();

    gen.generateData(USER_ID_MAX, userTable);
    userTable.release();
    fiji.release();
  }
}
