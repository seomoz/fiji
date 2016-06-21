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

import java.io.File;
import java.io.IOException;
import java.util.List;

import com.google.common.base.Preconditions;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.io.DatumReader;
import org.apache.avro.specific.SpecificDatumReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.moz.fiji.annotations.ApiAudience;
import com.moz.fiji.common.flags.Flag;
import com.moz.fiji.schema.InternalFijiError;
import com.moz.fiji.schema.KConstants;
import com.moz.fiji.schema.Fiji;
import com.moz.fiji.schema.FijiSystemTable;
import com.moz.fiji.schema.FijiURI;
import com.moz.fiji.schema.avro.MetadataBackup;
import com.moz.fiji.schema.impl.MetadataRestorer;
import com.moz.fiji.schema.util.ProtocolVersion;
import com.moz.fiji.schema.util.ResourceUtils;

/**
 * A tool to backup and restore Metadata.
 */
@ApiAudience.Private
public final class MetadataTool extends BaseTool {
  private static final Logger LOG = LoggerFactory.getLogger(MetadataTool.class);
  private MetadataRestorer mRestorer = new MetadataRestorer();

  @Flag(name="fiji", usage="URI of the Fiji instance to use.")
  private String mFijiURIFlag = KConstants.DEFAULT_INSTANCE_URI;

  @Flag(name = "backup", usage = "Output filename for Fiji metadata")
  private String mOutFile = null;

  @Flag(name = "restore", usage = "Input filename to restore Fiji metadata from")
  private String mInFile = null;

  // The following flags specify whether only certain parts of the backup should be restored.
  @Flag(name = "tables", usage = "Restore table definitions")
  private boolean mAllTables = false;

  @Flag(name = "schemas", usage = "Restore schema definitions")
  private boolean mSchemas = false;

  @Flag(name = "system-variables", usage = "Restore system table variables")
  private boolean mSystemVars = false;

  /** URI of the Fiji instance. */
  private FijiURI mFijiURI = null;

  /** {@inheritDoc} */
  @Override
  public String getName() {
    return "metadata";
  }

  /** {@inheritDoc} */
  @Override
  public String getDescription() {
    return "Backup or restore fiji metadata.";
  }

  /** {@inheritDoc} */
  @Override
  public String getCategory() {
    return "Metadata";
  }

  /**
   * Sets flags to indicate which aspects of a backup to restore.
   */
  private void initRestoreOps() {
    if (!mAllTables && !mSchemas && !mSystemVars) {
      // User has selected no restore operations. Restore everything.
      mAllTables = true;
      mSchemas = true;
      mSystemVars = true;
    }
  }

  /**
   * Restores Fiji metadata from an Avro file with the specified filename.
   *
   * @param inputFile the input filename.
   * @param fiji the Fiji instance to restore to.
   * @throws IOException when an error communicating with HBase or the file system occurs.
   */
  private void restoreMetadata(String inputFile, Fiji fiji) throws IOException {
    LOG.debug("Restoring metadata...");
    final File file = new File(inputFile);
    if (!file.exists()) {
      throw new IOException("No such metadata file: " + inputFile);
    }

    final DatumReader<MetadataBackup> datumReader =
        new SpecificDatumReader<MetadataBackup>(MetadataBackup.class);
    final DataFileReader<MetadataBackup> fileReader =
        new DataFileReader<MetadataBackup>(file, datumReader);

    try {
      if (!fileReader.hasNext()) {
        throw new IOException("Metadata file '" + inputFile + "' is empty");
      }

      LOG.debug("Reading metadata backup file entry...");
      final MetadataBackup backup = fileReader.next();
      LOG.debug("Read metadata backup file entry.");

      // Check that the dataVersion is compatible with this Fiji instance.
      final FijiSystemTable statusTable = fiji.getSystemTable();
      final ProtocolVersion curDataVersion = statusTable.getDataVersion();
      LOG.debug("Metadata backup data version: " + backup.getLayoutVersion());
      LOG.debug("Current data version: " + curDataVersion);
      if (!curDataVersion.equals(ProtocolVersion.parse(backup.getLayoutVersion()))) {
        throw new IOException(String.format(
          "Cannot restore: backup layout version '%s' does not match Fiji instance version '%s'.",
          backup.getLayoutVersion(), curDataVersion));
      }

      if (mAllTables) {
        LOG.debug("Restoring all tables...");
        mRestorer.restoreTables(backup, fiji);
      }

      if (mSchemas) {
        LOG.debug("Restoring schemas...");
        mRestorer.restoreSchemas(backup, fiji);
      }

      if (mSystemVars) {
        LOG.debug("Restoring system variables...");
        mRestorer.restoreSystemVars(backup, fiji);
      }
    } finally {
      fileReader.close();
    }

    getPrintStream().println("Restore complete.");
  }

  /** {@inheritDoc} */
  @Override
  protected void setup() throws Exception {
    Preconditions.checkArgument((mFijiURIFlag != null) && !mFijiURIFlag.isEmpty(),
        "Specify the Fiji instance to uninstall with --fiji=fiji://hbase-address/fiji-instance");
    mFijiURI = FijiURI.newBuilder(mFijiURIFlag).build();
    Preconditions.checkArgument(mFijiURI.getInstance() != null,
        "Specify the Fiji instance to uninstall with --fiji=fiji://hbase-address/fiji-instance");

    final boolean isRestore = (mInFile != null) && !mInFile.isEmpty();
    final boolean isBackup = (mOutFile != null) && !mOutFile.isEmpty();
    Preconditions.checkArgument(isRestore || isBackup, // Ensures that at least one is true
        "Specify exactly one of --backup and --restore.");
    Preconditions.checkArgument(!isRestore || !isBackup, // Ensures that at least one is false
        "Cannot specify both --backup and --restore simultaneously. "
        + "Specify exactly one of --backup and --restore.");
  }

  /** {@inheritDoc} */
  @Override
  protected int run(List<String> nonFlagArgs) throws Exception {
    final Fiji fiji = Fiji.Factory.open(mFijiURI, getConf());
    try {
      if ((mOutFile != null) && !mOutFile.isEmpty()) {
        try {
          mRestorer.exportMetadata(mOutFile, fiji);
        } catch (IOException ioe) {
          getPrintStream().println("Error performing backup: " + ioe.getMessage());
          getPrintStream().println("(Backup failed.)");
          return FAILURE;
        }
      } else if ((mInFile != null) && !mInFile.isEmpty()) {
        if (isInteractive()) {
          if (!yesNoPrompt("Are you sure you want to restore metadata from backup? \n"
            + "This will delete your current metatable. ")) {
            getPrintStream().println("No metadata restore operation performed.");
            return FAILURE;
          }
        }
        try {
          getPrintStream().println("Restoring Metadata from backup.");
          initRestoreOps();
          restoreMetadata(mInFile, fiji);

        } catch (IOException ioe) {
          getPrintStream().println("Error performing restore: " + ioe.getMessage());
          getPrintStream().println("(Restore failed.)");
          return FAILURE;

        } catch (Exception re) {
          getPrintStream().println(re.getMessage());
          getPrintStream().println("(Restore failed.)");
          return FAILURE;
        }
      } else {
        // Should be unreachable because preconditions catch this case.
        throw new InternalFijiError("Neither input nor output file specified.");
      }
    } finally {
      ResourceUtils.releaseOrLog(fiji);
    }
    return SUCCESS;
  }

  /**
   * Program entry point.
   *
   * @param args The command-line arguments.
   * @throws Exception If there is an error.
   */
  public static void main(String[] args) throws Exception {
    System.exit(new FijiToolLauncher().run(new MetadataTool(), args));
  }
}
