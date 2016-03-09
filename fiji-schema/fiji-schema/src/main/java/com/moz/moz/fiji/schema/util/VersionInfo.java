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

package com.moz.fiji.schema.util;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import com.google.common.base.Objects;
import org.apache.hadoop.hbase.TableNotFoundException;

import com.moz.fiji.annotations.ApiAudience;
import com.moz.fiji.annotations.ApiStability;
import com.moz.fiji.schema.IncompatibleFijiVersionException;
import com.moz.fiji.schema.Fiji;
import com.moz.fiji.schema.FijiNotInstalledException;
import com.moz.fiji.schema.FijiSystemTable;
import com.moz.fiji.schema.FijiURI;
import com.moz.fiji.schema.impl.Versions;

/**
 * Reports on the version numbers associated with this software bundle
 * as well as the installed format versions in used in a Fiji instance.
 */
@ApiAudience.Public
@ApiStability.Evolving
public final class VersionInfo {
  /** No constructor since this is a utility class. */
  private VersionInfo() {}

  /** Fallback software version ID, in case the properties file is not generated/reachable. */
  public static final String DEFAULT_DEVELOPMENT_VERSION = "development";

  private static final String FIJI_SCHEMA_PROPERTIES_RESOURCE =
      "com/moz/fiji/schema/fiji-schema.properties";

  private static final String FIJI_SCHEMA_VERSION_PROP_NAME = "fiji-schema-version";

  /**
   * Loads fiji schema properties.
   *
   * @return the Fiji schema properties.
   * @throws IOException on I/O error.
   */
  private static Properties loadFijiSchemaProperties() throws IOException {
    final InputStream istream =
        VersionInfo.class.getClassLoader().getResourceAsStream(FIJI_SCHEMA_PROPERTIES_RESOURCE);
    try {
      final Properties properties = new Properties();
      properties.load(istream);
      return properties;
    } finally {
      ResourceUtils.closeOrLog(istream);
    }
  }

  /**
   * Gets the version of the Fiji instance format installed on the HBase cluster.
   *
   * <p>The instance format describes the layout of the global metadata state of
   * a Fiji instance.</p>
   *
   * @param systemTable An open FijiSystemTable.
   * @return A parsed version of the storage format protocol version string.
   * @throws IOException on I/O error.
   */
  private static ProtocolVersion getClusterDataVersion(FijiSystemTable systemTable) throws
      IOException {
    try {
      final ProtocolVersion dataVersion = systemTable.getDataVersion();
      return dataVersion;
    } catch (TableNotFoundException e) {
      final FijiURI fijiURI = systemTable.getFijiURI();
      throw new FijiNotInstalledException(
          String.format("Fiji instance %s is not installed.", fijiURI),
          fijiURI);
    }
  }

  /**
   * Validates that the client instance format version is compatible with the instance
   * format version installed on a Fiji instance.
   * Returns true if they are compatible, false otherwise.
   * "Compatible" versions have the same major version digit (e.g., <tt>system-1.1</tt>
   * and <tt>system-1.0</tt> are compatible; <tt>system-2.5</tt> and <tt>system-1.0</tt> are not).
   *
   * <p>Older instances (installed with FijiSchema 1.0.0-rc3 and prior) will use an instance
   * format version of <tt>fiji-1.0</tt>. This is treated as an alias for <tt>system-1.0</tt>.
   * No other versions associated with the <tt>"fiji"</tt> protocol are supported.</p>
   *
   * @param systemTable An open FijiSystemTable.
   * @throws IOException on I/O error reading the Fiji version from the system.
   * @return true if the installed instance format
   *     version is compatible with this client, false otherwise.
   */
  private static boolean isFijiVersionCompatible(FijiSystemTable systemTable) throws IOException {
    final ProtocolVersion clientVersion = VersionInfo.getClientDataVersion();
    final ProtocolVersion clusterVersion = VersionInfo.getClusterDataVersion(systemTable);
    return areInstanceVersionsCompatible(clientVersion, clusterVersion);
  }

  /**
   * Gets the version of the Fiji client software.
   *
   * @return The version string.
   * @throws IOException on I/O error.
   */
  public static String getSoftwareVersion() throws IOException {
    final String version = VersionInfo.class.getPackage().getImplementationVersion();
    if (version != null) {
      // Proper release: use the value of 'Implementation-Version' in META-INF/MANIFEST.MF:
      return version;
    }

    // Most likely a development version:
    final Properties fijiProps = loadFijiSchemaProperties();
    return fijiProps.getProperty(FIJI_SCHEMA_VERSION_PROP_NAME, DEFAULT_DEVELOPMENT_VERSION);
  }

  /**
   * Reports the maximum system version of the Fiji instance format understood by the client.
   *
   * <p>
   *   The instance format describes the layout of the global metadata state of
   *   a Fiji instance. This version number specifies which Fiji instances it would
   *   be compatible with. See {@link #isFijiVersionCompatible} to determine whether
   *   a deployment is compatible with this version.
   * </p>
   *
   * @return A parsed version of the instance format protocol version string.
   */
  public static ProtocolVersion getClientDataVersion() {
    return Versions.MAX_SYSTEM_VERSION;
  }

  /**
   * Gets the version of the Fiji instance format installed on the HBase cluster.
   *
   * <p>The instance format describes the layout of the global metadata state of
   * a Fiji instance.</p>
   *
   * @param fiji An open fiji instance.
   * @return A parsed version of the storage format protocol version string.
   * @throws IOException on I/O error.
   */
  public static ProtocolVersion getClusterDataVersion(Fiji fiji) throws IOException {
    return getClusterDataVersion(fiji.getSystemTable());
  }

  /**
   * Validates that the client instance format version is compatible with the instance
   * format version installed on a Fiji instance.
   * Throws IncompatibleFijiVersionException if not.
   *
   * <p>For the definition of compatibility used in this method, see {@link
   * #isFijiVersionCompatible}</p>
   *
   * @param fiji An open fiji instance.
   * @throws IOException on I/O error reading the data version from the cluster,
   *     or throws IncompatibleFijiVersionException if the installed instance format version
   *     is incompatible with the version supported by the client.
   */
  public static void validateVersion(Fiji fiji) throws IOException {
    validateVersion(fiji.getSystemTable());
  }

  /**
   * Validates that the client instance format version is compatible with the instance
   * format version installed on a Fiji instance.
   * Throws IncompatibleFijiVersionException if not.
   *
   * This method may be useful if a Fiji is not fully constructed, but the FijiSystemTable exists,
   * such as during construction of a Fiji.  This method should only be used by framework-level
   * applications, since FijiSystemTable is a framework-level class.
   *
   * <p>For the definition of compatibility used in this method, see {@link
   * #isFijiVersionCompatible}</p>
   *
   * @param systemTable An open FijiSystemTable.
   * @throws IOException on I/O error reading the data version from the cluster,
   *     or throws IncompatibleFijiVersionException if the installed instance format version
   *     is incompatible with the version supported by the client.
   */
  public static void validateVersion(FijiSystemTable systemTable) throws IOException {
    if (isFijiVersionCompatible(systemTable)) {
      return; // valid.
    } else {
      final ProtocolVersion clientVersion = VersionInfo.getClientDataVersion();
      final ProtocolVersion clusterVersion = VersionInfo.getClusterDataVersion(systemTable);
      throw new IncompatibleFijiVersionException(String.format(
          "Data format of Fiji instance (%s) cannot operate with client (%s)",
          clusterVersion, clientVersion));
    }
  }

  /**
   * Validates that the client instance format version is compatible with the instance
   * format version installed on a Fiji instance.
   * Returns true if they are compatible, false otherwise.
   * "Compatible" versions have the same major version digit (e.g., <tt>system-1.1</tt>
   * and <tt>system-1.0</tt> are compatible; <tt>system-2.5</tt> and <tt>system-1.0</tt> are not).
   *
   * <p>Older instances (installed with FijiSchema 1.0.0-rc3 and prior) will use an instance
   * format version of <tt>fiji-1.0</tt>. This is treated as an alias for <tt>system-1.0</tt>.
   * No other versions associated with the <tt>"fiji"</tt> protocol are supported.</p>
   *
   * @param fiji An open fiji instance.
   * @throws IOException on I/O error reading the Fiji version from the system.
   * @return true if the installed instance format
   *     version is compatible with this client, false otherwise.
   */
  public static boolean isFijiVersionCompatible(Fiji fiji) throws IOException {
    final ProtocolVersion clientVersion = VersionInfo.getClientDataVersion();
    final ProtocolVersion clusterVersion = VersionInfo.getClusterDataVersion(fiji);
    return areInstanceVersionsCompatible(clientVersion, clusterVersion);
  }

  /**
   * Actual comparison logic that validates client/cluster data compatibility according to
   * the rules defined in {@link #isFijiVersionCompatible(Fiji)}.
   *
   * <p>package-level visibility for unit testing.</p>
   *
   * @param clientVersion the client software's instance version.
   * @param clusterVersion the cluster's installed instance version.
   * @return true if the installed instance format
   *     version is compatible with this client, false otherwise.
   */
  static boolean areInstanceVersionsCompatible(
      ProtocolVersion clientVersion, ProtocolVersion clusterVersion) {

    if (Objects.equal(clusterVersion, Versions.SYSTEM_FIJI_1_0_DEPRECATED)) {
      // The "fiji-1.0" version is equivalent to "system-1.0" in compatibility tests.
      clusterVersion = Versions.SYSTEM_1_0;
    }

    // See SCHEMA-469.
    // From https://github.com/fijiproject/wiki/wiki/FijiVersionCompatibility:
    //
    // (quote)
    //   Our primary goal with data formats is backwards compatibility. FijiSchema Version 1.x
    //   will read and interact with data written by Version 1.y, if x > y.
    //
    // ...
    //
    //   The instance format describes the feature set and format of the entire Fiji instance.
    //   This allows a client to determine whether it can connect to the instance at all, read
    //   any cells, and identifies where other metadata (e.g., schemas, layouts) is located, as
    //   well as the range of supported associated formats within that metadata.
    //
    //   The fiji version command specifies the instance version presently installed on the
    //   cluster, and the system data version supported by the client. The major version numbers
    //   of these must agree to connect to one another.
    // (end quote)
    //
    // Given these requirements, we require that our client's supported instance version major
    // digit be greater than or equal to the major digit of the system installation.
    //
    // If we ever deprecate an instance format and then target it for end-of-life (i.e., when
    // contemplating FijiSchema 2.0.0), we may introduce a cut-off point: we may support only
    // the most recent 'k' major digits; so FijiSchema 2.0.0 may have system-12.0, but support
    // reading/writing instances only as far back as system-9.0. This is not yet implemented;
    // if we do go down this route, then this method would be the place to do it.

    return clientVersion.getProtocolName().equals(clusterVersion.getProtocolName())
        && clientVersion.getMajorVersion() >= clusterVersion.getMajorVersion();
  }
}
