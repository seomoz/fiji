/**
 * (c) Copyright 2014 WibiData, Inc.
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

package com.moz.fiji.schema.hbase;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Collection;

import com.moz.fiji.annotations.ApiAudience;
import com.moz.fiji.annotations.ApiStability;
import com.moz.fiji.schema.InternalFijiError;
import com.moz.fiji.schema.KConstants;
import com.moz.fiji.schema.FijiColumnName;
import com.moz.fiji.schema.FijiURI;
import com.moz.fiji.schema.FijiURIException;
import com.moz.fiji.schema.impl.FijiURIParser;
import com.moz.fiji.schema.impl.hbase.HBaseFijiFactory;
import com.moz.fiji.schema.impl.hbase.HBaseFijiInstaller;

/**
 * a {@link FijiURI} that uniquely identifies an HBase Fiji instance, table, and column-set.
 *
 * <h2>{@code HBaseFijiURI} Scheme</h2>
 *
 * The scheme for {@code HBaseFijiURI}s is either {@code fiji} or {@code fiji-hbase}. When either
 * of these schemes is used while parsing a {@code FijiURI}, the resulting URI or builder will
 * be an HBase specific type.
 *
 * <h2>{@code HBaseFijiURI} Cluster Identifier</h2>
 *
 * HBase Fiji needs only a valid ZooKeeper ensemble in order to identify the host HBase cluster.
 * The cluster identifier is identical to the default FijiURI cluster identifier.
 * <p>
 * The {@value #ENV_URI_STRING} value will resolve at runtime to the ZooKeeper ensemble address
 * in the HBase configuration on the classpath.
 *
 * <H2>{@code HBaseFijiURI} Examples</H2>
 *
 * The following are valid example {@code HBaseFijiURI}s:
 *
 * <li> {@code fiji-hbase://zkHost}
 * <li> {@code fiji-hbase://zkHost/instance}
 * <li> {@code fiji-hbase://zkHost/instance/table}
 * <li> {@code fiji-hbase://zkHost:zkPort/instance/table}
 * <li> {@code fiji-hbase://zkHost1,zkHost2/instance/table}
 * <li> {@code fiji-hbase://(zkHost1,zkHost2):zkPort/instance/table}
 * <li> {@code fiji-hbase://zkHost/instance/table/col}
 * <li> {@code fiji-hbase://zkHost/instance/table/col1,col2}
 * <li> {@code fiji-hbase://.env/instance/table}
 * <li> {@code fiji-hbase://.unset/instance/table}
 */
@ApiAudience.Framework
@ApiStability.Evolving
public final class HBaseFijiURI extends FijiURI {

  /** URI scheme used to fully qualify an HBase Fiji instance. */
  public static final String HBASE_SCHEME = "fiji-hbase";

  /**
   * Constructs a new HBaseFijiURI with the given parameters.
   *
   * @param scheme of the URI.
   * @param zookeeperQuorum Zookeeper quorum.
   * @param zookeeperClientPort Zookeeper client port.
   * @param instanceName Instance name.
   * @param tableName Table name.
   * @param columnNames Column names.
   * @throws FijiURIException If the parameters are invalid.
   */
  private HBaseFijiURI(
      final String scheme,
      final Iterable<String> zookeeperQuorum,
      final int zookeeperClientPort,
      final String instanceName,
      final String tableName,
      final Iterable<FijiColumnName> columnNames) {
    super(scheme, zookeeperQuorum, zookeeperClientPort, instanceName, tableName, columnNames);
  }

  /**
   * Builder class for constructing HBaseFijiURIs.
   */
  public static final class HBaseFijiURIBuilder extends FijiURIBuilder {

    /**
     * Constructs a new builder for HBaseFijiURIs.
     *
     * @param scheme of the URI.
     * @param zookeeperQuorum The initial zookeeper quorum.
     * @param zookeeperClientPort The initial zookeeper client port.
     * @param instanceName The initial instance name.
     * @param tableName The initial table name.
     * @param columnNames The initial column names.
     */
    private HBaseFijiURIBuilder(
        final String scheme,
        final Iterable<String> zookeeperQuorum,
        final int zookeeperClientPort,
        final String instanceName,
        final String tableName,
        final Iterable<FijiColumnName> columnNames) {
      super(scheme, zookeeperQuorum, zookeeperClientPort, instanceName, tableName, columnNames);
    }

    /**
     * Constructs a new builder for HBaseFijiURIs.
     */
    public HBaseFijiURIBuilder() {
      super();
    }

    /**
     * Overridden to provide specific return type.
     *
     * {@inheritDoc}
     */
    @Override
    public HBaseFijiURIBuilder withZookeeperQuorum(String[] zookeeperQuorum) {
      super.withZookeeperQuorum(zookeeperQuorum);
      return this;
    }

    /**
     * Overridden to provide specific return type.
     *
     * {@inheritDoc}
     */
    @Override
    public HBaseFijiURIBuilder withZookeeperQuorum(Iterable<String> zookeeperQuorum) {
      super.withZookeeperQuorum(zookeeperQuorum);
      return this;
    }

    /**
     * Overridden to provide specific return type.
     *
     * {@inheritDoc}
     */
    @Override
    public HBaseFijiURIBuilder withZookeeperClientPort(int zookeeperClientPort) {
      super.withZookeeperClientPort(zookeeperClientPort);
      return this;
    }

    /**
     * Overridden to provide specific return type.
     *
     * {@inheritDoc}
     */
    @Override
    public HBaseFijiURIBuilder withInstanceName(String instanceName) {
      super.withInstanceName(instanceName);
      return this;
    }

    /**
     * Overridden to provide specific return type.
     *
     * {@inheritDoc}
     */
    @Override
    public HBaseFijiURIBuilder withTableName(String tableName) {
      super.withTableName(tableName);
      return this;
    }

    /**
     * Overridden to provide specific return type.
     *
     * {@inheritDoc}
     */
    @Override
    public HBaseFijiURIBuilder withColumnNames(Collection<String> columnNames) {
      super.withColumnNames(columnNames);
      return this;
    }

    /**
     * Overridden to provide specific return type.
     *
     * {@inheritDoc}
     */
    @Override
    public HBaseFijiURIBuilder addColumnNames(Collection<FijiColumnName> columnNames) {
      super.addColumnNames(columnNames);
      return this;
    }

    /**
     * Overridden to provide specific return type.
     *
     * {@inheritDoc}
     */
    @Override
    public HBaseFijiURIBuilder addColumnName(FijiColumnName columnName) {
      super.addColumnName(columnName);
      return this;
    }

    /**
     * Overridden to provide specific return type.
     *
     * {@inheritDoc}
     */
    @Override
    public HBaseFijiURIBuilder withColumnNames(Iterable<FijiColumnName> columnNames) {
      super.withColumnNames(columnNames);
      return this;
    }

    /**
     * Builds the configured HBaseFijiURI.
     *
     * @return A HBaseFijiURI.
     * @throws FijiURIException If the HBaseFijiURI was configured improperly.
     */
    @Override
    public HBaseFijiURI build() {
      return new HBaseFijiURI(
          mScheme,
          mZookeeperQuorum,
          mZookeeperClientPort,
          mInstanceName,
          mTableName,
          mColumnNames);
    }
  }

  /**
   * A {@link FijiURIParser} for {@link HBaseFijiURI}s.
   */
  public static final class HBaseFijiURIParser implements FijiURIParser {
    /** {@inheritDoc} */
    @Override
    public HBaseFijiURIBuilder parse(final URI uri) {
      final ZooKeeperAuthorityParser authorityParser =
          ZooKeeperAuthorityParser.getAuthorityParser(uri);
      final PathParser segmentParser = new PathParser(uri);

      return new HBaseFijiURIBuilder(
          uri.getScheme(),
          authorityParser.getZookeeperQuorum(),
          authorityParser.getZookeeperClientPort(),
          segmentParser.getInstance(),
          segmentParser.getTable(),
          segmentParser.getColumns());
    }

    @Override
    public String getName() {
      return HBASE_SCHEME;
    }
  }

  /** {@inheritDoc} */
  @Override
  public int hashCode() {
    return toString().hashCode();
  }

  /** {@inheritDoc} */
  @Override
  public boolean equals(Object object) {
    if (object == null) {
      return false;
    }
    return object.getClass() == this.getClass() && object.toString().equals(this.toString());
  }

  /**
   * Gets a builder configured with default Fiji URI fields.
   *
   * More precisely, the following defaults are initialized:
   * <ul>
   *   <li>The Zookeeper quorum and client port is taken from the Hadoop <tt>Configuration</tt></li>
   *   <li>The Fiji instance name is set to <tt>KConstants.DEFAULT_INSTANCE_NAME</tt>
   *       (<tt>"default"</tt>).</li>
   *   <li>The table name and column names are explicitly left unset.</li>
   * </ul>
   *
   * @return A builder configured with this Fiji URI.
   */
  public static HBaseFijiURIBuilder newBuilder() {
    return new HBaseFijiURIBuilder();
  }

  /**
   * Gets a builder configured with a Fiji URI.
   *
   * @param uri The Fiji URI to configure the builder from.
   * @return A builder configured with uri.
   */
  public static HBaseFijiURIBuilder newBuilder(HBaseFijiURI uri) {
    return new HBaseFijiURIBuilder(
        uri.getScheme(),
        uri.getZookeeperQuorumOrdered(),
        uri.getZookeeperClientPort(),
        uri.getInstance(),
        uri.getTable(),
        uri.getColumnsOrdered());
  }

  /**
   * Gets a builder configured with the Fiji URI.
   *
   * <p> The String parameter can be a relative URI (with a specified instance), in which
   *     case it is automatically normalized relative to DEFAULT_HBASE_URI.
   *
   * @param uri String specification of a Fiji URI.
   * @return A builder configured with uri.
   * @throws FijiURIException If the uri is invalid.
   */
  public static HBaseFijiURIBuilder newBuilder(final String uri) {
    final String uriWithScheme;
    if (uri.startsWith(FIJI_SCHEME) || uri.startsWith(HBASE_SCHEME)) {
      uriWithScheme = uri;
    } else {
      uriWithScheme = String.format("%s/%s/", KConstants.DEFAULT_HBASE_URI, uri);
    }
    try {
      return new HBaseFijiURIParser().parse(new URI(uriWithScheme));
    } catch (URISyntaxException exn) {
      throw new FijiURIException(uri, exn.getMessage());
    }
  }

  /**
   * Overridden to provide specific return type.
   *
   * {@inheritDoc}
   */
  @Override
  public HBaseFijiURI resolve(String path) {
    try {
      // Without the "./", URI will assume a path containing a colon
      // is a new URI, for example "family:column".
      URI uri = new URI(toString()).resolve(String.format("./%s", path));
      return newBuilder(uri.toString()).build();
    } catch (URISyntaxException e) {
      // This should never happen
      throw new InternalFijiError(String.format("FijiURI was incorrectly constructed: %s.", this));
    } catch (IllegalArgumentException e) {
      throw new FijiURIException(this.toString(),
          String.format("Path can not be resolved: %s", path));
    }
  }

  /** {@inheritDoc} */
  @Override
  protected HBaseFijiURIBuilder getBuilder() {
    return newBuilder(this);
  }

  /** {@inheritDoc} */
  @Override
  protected HBaseFijiFactory getFijiFactory() {
    return new HBaseFijiFactory();
  }

  /** {@inheritDoc} */
  @Override
  protected HBaseFijiInstaller getFijiInstaller() {
    return HBaseFijiInstaller.get();
  }
}
