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

package com.moz.fiji.schema.tools;

import java.io.IOException;
import java.util.List;
import java.util.Random;

import com.google.common.base.Preconditions;

import com.moz.fiji.annotations.ApiAudience;
import com.moz.fiji.common.flags.Flag;
import com.moz.fiji.schema.EntityId;
import com.moz.fiji.schema.Fiji;
import com.moz.fiji.schema.FijiTable;
import com.moz.fiji.schema.FijiTableWriter;
import com.moz.fiji.schema.FijiURI;
import com.moz.fiji.schema.tools.synth.DictionaryLoader;
import com.moz.fiji.schema.tools.synth.EmailSynthesizer;
import com.moz.fiji.schema.tools.synth.NGramSynthesizer;
import com.moz.fiji.schema.tools.synth.WordSynthesizer;
import com.moz.fiji.schema.util.ResourceUtils;

/**
 * Synthesize some user data into a fiji table.
 */
@ApiAudience.Private
public final class SynthesizeUserDataTool extends BaseTool {
  @Flag(name="name-dict", usage="File that contains people names, one per line")
  private String mNameDictionaryFilename = "com.moz.fiji/schema/tools/synth/top_names.txt";

  @Flag(name="num-users", usage="Number of users to synthesize")
  private int mNumUsers = 100;

  @Flag(name="table", usage="URI of the Fiji table data should be written to.")
  private String mTableURIFlag = null;

  /** URI of the target table to write to. */
  private FijiURI mTableURI = null;

  /** Fiji instance where the target table lives. */
  private Fiji mFiji= null;

  /** Fiji table to write to. */
  private FijiTable mTable = null;


  /** {@inheritDoc} */
  @Override
  public String getName() {
    return "synthesize-user-data";
  }

  /** {@inheritDoc} */
  @Override
  public String getDescription() {
    return "Synthesize user data into a fiji table.";
  }

  /** {@inheritDoc} */
  @Override
  public String getCategory() {
    return "Example";
  }

  /**
   * Load a list of people names from a file.
   *
   * @param filename The name of the file containing a dictionary of words, one per line.
   * @return The list of words from the file.
   * @throws IOException If there is an error.
   */
  private List<String> loadNameDictionary(String filename)
      throws IOException {
    DictionaryLoader loader = new DictionaryLoader();
    return loader.load(getClass().getClassLoader().getResourceAsStream(filename));
  }

  /** {@inheritDoc} */
  @Override
  protected void validateFlags() throws Exception {
    super.validateFlags();
    Preconditions.checkArgument((mTableURIFlag != null) && !mTableURIFlag.isEmpty(),
        "Specify a target table to write synthesized data to with "
        + "--table=fiji://hbase-address/fiji-instance/table");
    mTableURI = FijiURI.newBuilder(mTableURIFlag).build();
    Preconditions.checkArgument(mTableURI.getTable() != null,
        "No table specified in target URI '%s'. "
        + "Specify a target table to write synthesized data to with "
        + "--table=fiji://hbase-address/fiji-instance/table",
        mTableURI);
  }

  /** {@inheritDoc} */
  @Override
  protected void setup() throws IOException {
    mFiji = Fiji.Factory.open(mTableURI, getConf());
    mTable = mFiji.openTable(mTableURI.getTable());
  }

  /** {@inheritDoc} */
  @Override
  protected void cleanup() throws IOException {
    ResourceUtils.releaseOrLog(mTable);
    ResourceUtils.releaseOrLog(mFiji);

    mTableURI = null;
    mTable = null;
    mFiji = null;
  }

  /** {@inheritDoc} */
  @Override
  protected int run(List<String> nonFlagArgs) throws Exception {
    // Generate a bunch of user rows with names and email addresses.
    final Random random = new Random(System.currentTimeMillis());
    final List<String> nameDictionary = loadNameDictionary(mNameDictionaryFilename);
    final WordSynthesizer nameSynth = new WordSynthesizer(random, nameDictionary);
    final NGramSynthesizer fullNameSynth = new NGramSynthesizer(nameSynth, 2);
    final EmailSynthesizer emailSynth = new EmailSynthesizer(random, nameDictionary);

    getPrintStream().printf("Generating %d users on fiji table '%s'...%n", mNumUsers, mTableURI);
    final FijiTableWriter tableWriter = mTable.openTableWriter();
    try {
      for (int iuser = 0; iuser < mNumUsers; iuser++) {
        final String fullName = fullNameSynth.synthesize();
        final String email =
            EmailSynthesizer.formatEmail(fullName.replace(" ", "."), emailSynth.synthesizeDomain());
        final EntityId entityId = mTable.getEntityId(email);
        tableWriter.put(entityId, "info", "name", fullName);
        tableWriter.put(entityId, "info", "email", email);

        // Print some status so the user knows it's working.
        if (iuser % 1000 == 0) {
          getPrintStream().printf("%d rows synthesized...%n", iuser);
        }
      }
    } finally {
      ResourceUtils.closeOrLog(tableWriter);
    }

    getPrintStream().printf("%d rows synthesized...%n", mNumUsers);
    getPrintStream().println("Done.");

    return SUCCESS;
  }

  /**
   * Program entry point.
   *
   * @param args The command-line arguments.
   * @throws Exception If there is an error.
   */
  public static void main(String[] args) throws Exception {
    System.exit(new FijiToolLauncher().run(new SynthesizeUserDataTool(), args));
  }
}
