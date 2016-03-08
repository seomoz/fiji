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

package com.moz.fiji.schema;

import java.io.IOException;
import java.io.PrintStream;
import java.util.List;

import org.apache.hadoop.conf.Configuration;

import com.moz.fiji.annotations.ApiAudience;
import com.moz.fiji.annotations.ApiStability;
import com.moz.fiji.annotations.Inheritance;
import com.moz.fiji.commons.ReferenceCountable;
import com.moz.fiji.schema.avro.TableLayoutDesc;
import com.moz.fiji.schema.impl.hbase.HBaseFijiFactory;
import com.moz.fiji.schema.layout.FijiTableLayout;
import com.moz.fiji.schema.security.FijiSecurityManager;

/**
 * <p>Provides a handle to a Fiji instance that contains table information and access to
 * Fiji administrative functionality.  Fiji instances are identified by a {@link FijiURI}.
 * The default FijiURI is: <em>fiji://.env/default/</em></p>
 *
 * <h2>Fiji instance lifecycle:</h2>
 * An instance to Fiji can be retrieved using {@link Fiji.Factory#open(FijiURI)}.  Cleanup is
 * requested with the inherited {@link #release()} method.  Thus the lifecycle of a Fiji
 * object looks like:
 * <pre><code>
 *   Fiji fiji = Fiji.Factory.open(fijiURI);
 *   // Do some magic
 *   fiji.release();
 * </code></pre>
 *
 * <h2>Base Fiji tables within an instance:</h2>
 * <p>
 *   Upon installation, a Fiji instance contains a {@link FijiSystemTable}, {@link FijiMetaTable},
 *   and {@link FijiSchemaTable} that each contain information relevant to the operation of Fiji.
 * </p>
 * <ul>
 *   <li>System table: Fiji system and version information.</li>
 *   <li>Meta table: Metadata about the existing Fiji tables.</li>
 *   <li>Schema table: Avro Schemas of the cells in Fiji tables.</li>
 * </ul>
 * <p>
 *   These tables can be accessed via the {@link #getSystemTable()}, {@link #getMetaTable()},
 *   and {@link #getSchemaTable()} methods respectively.
 * </p>
 *
 * <h2>User tables:</h2>
 * <p>
 *   User tables can be accessed using {@link #openTable(String)}.  Note that they must be closed
 *   after they are no longer necessary.
 * </p>
 * <pre><code>
 *   FijiTable fijiTable = fiji.openTable("myTable");
 *   // Do some table operations
 *   fijiTable.close();
 * </code></pre>
 *
 * <h2>Administration of user tables:</h2>
 * <p>
 *   The Fiji administration interface contains methods to create, modify, and delete tables from a
 *   Fiji instance:
 * </p>
 * <ul>
 *   <li>{@link #createTable(TableLayoutDesc)} - creates a Fiji table with a specified layout.</li>
 *   <li>{@link #modifyTableLayout(TableLayoutDesc)} - updates a Fiji table with a new layout.</li>
 *   <li>{@link #deleteTable(String)} - removes a Fiji table from HBase.</li>
 * </ul>
 */
@ApiAudience.Public
@ApiStability.Stable
@Inheritance.Sealed
public interface Fiji extends FijiTableFactory, ReferenceCountable<Fiji> {
  /**
   * Provider for the default Fiji factory.
   *
   * Ensures that there is only one FijiFactory instance.
   */
  public static final class Factory {

    /**
     * Get an HBase-backed FijiFactory instance.
     *
     * @return a default HBase FijiFactory.
     * @deprecated use {@link #get(FijiURI)} instead.
     */
    @Deprecated
    public static FijiFactory get() {
      return new HBaseFijiFactory();
    }

    /**
     * Returns a FijiFactory for the appropriate Fiji type of the URI.
     *
     * @param uri for the Fiji instance to build with the factory.
     * @return the default FijiFactory.
     */
    public static FijiFactory get(FijiURI uri) {
      return uri.getFijiFactory();
    }

    /**
     * Opens a Fiji instance by URI.
     * <p>
     * Caller does not need to call {@code Fiji.retain()}, but must call {@code Fiji.release()}
     * after the returned {@code Fiji} will no longer be used.
     *
     * @param uri URI specifying the Fiji instance to open.
     * @return the specified Fiji instance.
     * @throws IOException on I/O error.
     */
    public static Fiji open(FijiURI uri) throws IOException {
      return get(uri).open(uri);
    }

    /**
     * Opens a Fiji instance by URI.
     * <p>
     * Caller does not need to call {@code Fiji.retain()}, but must call {@code Fiji.release()}
     * after the returned {@code Fiji} will no longer be used.
     *
     * @param uri URI specifying the Fiji instance to open.
     * @param conf Hadoop configuration.
     * @return the specified Fiji instance.
     * @throws IOException on I/O error.
     */
    public static Fiji open(FijiURI uri, Configuration conf) throws IOException {
      return get(uri).open(uri, conf);
    }

    /** Utility class may not be instantiated. */
    private Factory() {
    }
  }

  /** @return The hadoop configuration. */
  @Deprecated
  Configuration getConf();

  /** @return The address of this fiji instance, trimmed to the Fiji instance path component. */
  FijiURI getURI();

  /**
   * Gets the schema table for this Fiji instance.
   *
   * <p> The schema table is owned by the Fiji instance : do not close it. </p>.
   *
   * @return The fiji schema table.
   * @throws IOException If there is an error.
   */
  FijiSchemaTable getSchemaTable() throws IOException;

  /**
   * Gets the system table for this Fiji instance.
   *
   * <p> The system table is owned by the Fiji instance : do not close it. </p>.
   *
   * @return The fiji system table.
   * @throws IOException If there is an error.
   */
  FijiSystemTable getSystemTable() throws IOException;

  /**
   * Gets the meta table for this Fiji instance.
   *
   * <p> The meta table is owned by the Fiji instance : do not close it. </p>.
   *
   * @return The fiji meta table.
   * @throws IOException If there is an error.
   */
  FijiMetaTable getMetaTable() throws IOException;

  /**
   * Returns whether security is enabled for this Fiji.
   *
   * @return whether security is enabled for this Fiji.
   * @throws IOException If there is an error.
   */
  boolean isSecurityEnabled() throws IOException;

  /**
   * Gets the security manager for this Fiji.  This method creates a new FijiSecurityManager if one
   * doesn't exist for this instance already.
   *
   * <p>Throws FijiSecurityException if the Fiji security version is not compatible with a
   * FijiSecurityManager.  Use {@link #isSecurityEnabled()} to check whether security version
   * is compatible first.</p>
   *
   * @return The FijiSecurityManager for this Fiji.
   * @throws IOException If there is an error opening the FijiSecurityManager.
   */
  FijiSecurityManager getSecurityManager() throws IOException;

  /**
   * Creates a Fiji table in an HBase instance.
   *
   * @param name The name of the table to create.
   * @param layout The initial layout of the table (with unassigned column ids).
   * @throws IOException on I/O error.
   * @throws FijiAlreadyExistsException if the table already exists.
   * @deprecated Replaced by {@link #createTable(TableLayoutDesc)}
   */
  @Deprecated
  void createTable(String name, FijiTableLayout layout) throws IOException;

  /**
   * Creates a Fiji table in an HBase instance.
   *
   * @param layout The initial layout of the table (with unassigned column ids).
   * @throws IOException on I/O error.
   * @throws FijiAlreadyExistsException if the table already exists.
   */
  void createTable(TableLayoutDesc layout) throws IOException;

  /**
   * Creates a Fiji table in an HBase instance.
   *
   * @param name The name of the table to create.
   * @param layout The initial layout of the table (with unassigned column ids).
   * @param numRegions The initial number of regions to create.
   * @throws IllegalArgumentException If numRegions is non-positive, or if numRegions is
   *     more than 1 and row key hashing on the table layout is disabled.
   * @throws IOException on I/O error.
   * @throws FijiAlreadyExistsException if the table already exists.
   * @deprecated Replaced by {@link #createTable(TableLayoutDesc, int)}
   */
  @Deprecated
  void createTable(String name, FijiTableLayout layout, int numRegions) throws IOException;

  /**
   * Creates a Fiji table in an HBase instance.
   *
   * @param layout The initial layout of the table (with unassigned column ids).
   * @param numRegions The initial number of regions to create.
   * @throws IllegalArgumentException If numRegions is non-positive, or if numRegions is
   *     more than 1 and row key hashing on the table layout is disabled.
   * @throws IOException on I/O error.
   * @throws FijiAlreadyExistsException if the table already exists.
   */
  void createTable(TableLayoutDesc layout, int numRegions) throws IOException;

  /**
   * Creates a Fiji table in an HBase instance.
   *
   * @param name The name of the table to create.
   * @param layout The initial layout of the table (with unassigned column ids).
   * @param splitKeys The initial key boundaries between regions.  There will be splitKeys
   *     + 1 regions created.  Pass null to specify the default single region.
   * @throws IOException on I/O error.
   * @throws FijiAlreadyExistsException if the table already exists.
   * @deprecated Replaced by {@link #createTable(TableLayoutDesc, byte[][])}
   */
  @Deprecated
  void createTable(String name, FijiTableLayout layout, byte[][] splitKeys) throws IOException;

  /**
   * Creates a Fiji table in an HBase instance.
   *
   * @param layout The initial layout of the table (with unassigned column ids).
   * @param splitKeys The initial key boundaries between regions.  There will be splitKeys
   *     + 1 regions created.  Pass null to specify the default single region.
   * @throws IOException on I/O error.
   * @throws FijiAlreadyExistsException if the table already exists.
   */
  void createTable(TableLayoutDesc layout, byte[][] splitKeys) throws IOException;

  /**
   * Deletes a Fiji table.  Removes it from HBase.
   *
   * @param name The name of the Fiji table to delete.
   * @throws IOException If there is an error.
   */
  void deleteTable(String name) throws IOException;

  /**
   * Gets the list of Fiji table names.
   *
   * @return A list of the names of Fiji tables installed in the Fiji instance.
   * @throws IOException If there is an error.
   */
  List<String> getTableNames() throws IOException;

  /**
   * Sets the layout of a table.
   *
   * @param name The name of the Fiji table to change the layout of.
   * @param update The new layout for the table.
   * @return the updated layout.
   * @throws IOException If there is an error.
   * @deprecated Replaced by {@link #modifyTableLayout(TableLayoutDesc)}.
   */
  @Deprecated
  FijiTableLayout modifyTableLayout(String name, TableLayoutDesc update) throws IOException;

  /**
   * Sets the layout of a table.
   *
   * @param update The new layout for the table.
   * @return the updated layout.
   * @throws IOException If there is an error.
   */
  FijiTableLayout modifyTableLayout(TableLayoutDesc update) throws IOException;

  /**
   * Sets the layout of a table, or print the results of setting the table layout if
   * dryRun is true.
   *
   * @param name The name of the Fiji table to affect.
   * @param update The new layout for the table.
   * @param dryRun true if this is a 'dry run', false if changes should be made.
   * @param printStream the print stream to use for information if dryRun is true.
   *     If null, stdout will be used.
   * @return the updated layout.
   * @throws IOException If there is an error.
   * @deprecated Replaced by {@link #modifyTableLayout(TableLayoutDesc, boolean, PrintStream)}.
   */
  @Deprecated
  FijiTableLayout modifyTableLayout(
      String name, TableLayoutDesc update, boolean dryRun, PrintStream printStream)
      throws IOException;

  /**
   * Sets the layout of a table, or print the results of setting the table layout if
   * dryRun is true.
   *
   * @param update The new layout for the table.
   * @param dryRun true if this is a 'dry run', false if changes should be made.
   * @param printStream the print stream to use for information if dryRun is true.
   *     If null, stdout will be used.
   * @return the updated layout.
   * @throws IOException If there is an error.
   */
  FijiTableLayout modifyTableLayout(
      TableLayoutDesc update, boolean dryRun, PrintStream printStream)
      throws IOException;
}
