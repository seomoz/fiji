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

package com.moz.fiji.schema.cassandra;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Collection;
import java.util.List;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSortedSet;

import com.moz.fiji.annotations.ApiAudience;
import com.moz.fiji.annotations.ApiStability;
import com.moz.fiji.schema.InternalFijiError;
import com.moz.fiji.schema.FijiColumnName;
import com.moz.fiji.schema.FijiURI;
import com.moz.fiji.schema.FijiURIException;
import com.moz.fiji.schema.impl.FijiURIParser;
import com.moz.fiji.schema.impl.cassandra.CassandraFijiFactory;
import com.moz.fiji.schema.impl.cassandra.CassandraFijiInstaller;

/**
 * a {@link FijiURI} that uniquely identifies a Cassandra Fiji instance, table, and column set.
 *
 * <h2>{@code CassandraFijiURI} Scheme</h2>
 *
 * The scheme for {@code CassandraFijiURI}s is {@code fiji-cassandra}. When a parsing a FijiURI with
 * this scheme, the resulting {@code URI} or builder will be a Cassandra specific type.
 *
 * <h2>{@code CassandraFijiURI} Cluster Identifier</h2>
 *
 * Cassandra Fiji needs a valid ZooKeeper ensemble, as well as a set of contact points in the
 * Cassandra cluster. The ZooKeeper ensemble is specified in the same way as the default
 * {@code FijiURI}. The contact points follow the ZooKeeper ensemble separated with a slash.  The
 * contact points take one of the following forms, depending on whether one or more hosts is
 * specified, and whether a port is specified:
 *
 * <li> {@code host}
 * <li> {@code host:port}
 * <li> {@code host1,host2}
 * <li> {@code (host1,host2):port}
 *
 * <H2>{@code CassandraFijiURI} Examples</H2>
 *
 * The following are valid example {@code CassandraFijiURI}s:
 *
 * <li> {@code fiji-cassandra://zkEnsemble/contactPoint}
 * <li> {@code fiji-cassandra://zkEnsemble/contactPoint,contactPoint}
 * <li> {@code fiji-cassandra://zkEnsemble/contactPoint:9042/instance}
 * <li> {@code fiji-cassandra://zkEnsemble/contactPoint:9042/instance/table}
 * <li> {@code fiji-cassandra://zkEnsemble/contactPoint:9042/instance/table/col1,col2}
 * <li> {@code fiji-cassandra://zkEnsemble/(contactPoint,contactPoint):9042}
 * <li> {@code fiji-cassandra://zkEnsemble/(contactPoint,contactPoint):9042/instance/table}
 */
@ApiAudience.Framework
@ApiStability.Evolving
public final class CassandraFijiURI extends FijiURI {

  /** URI scheme used to fully qualify a Cassandra Fiji instance. */
  public static final String CASSANDRA_SCHEME = "fiji-cassandra";

  /** Default Cassandra contact host. */
  private static final String DEFAULT_CONTACT_POINT = "127.0.0.1";

  /** Default Cassandra contact port. */
  private static final int DEFAULT_CONTACT_PORT = 9042;

  /**
   * Cassandra contact point host names or IP addresses. preserves user specified ordering.
   * Not null.
   */
  private final ImmutableList<String> mContactPoints;

  /** Normalized (sorted and de-duplicated) contact point host names or IP addresses. Not null. */
  private final ImmutableList<String> mContactPointsNormalized;

  /** Cassandra contact points port number. */
  private final int mContactPort;

  private final String mUsername;
  private final String mPassword;

  // CSOFF: ParameterNumberCheck
  /**
   * Constructs a new CassandraFijiURI with the given parameters.
   *
   * @param scheme of the URI.
   * @param zookeeperQuorum Zookeeper quorum.
   * @param zookeeperClientPort Zookeeper client port.
   * @param contactPoints The host names of Cassandra contact points.
   * @param contactPort The port of Cassandra contact points.
   * @param username Optional username for Cassandra authentication.
   * @param password Optional password for Cassandra authentication.
   * @param instanceName Instance name.
   * @param tableName Table name.
   * @param columnNames Column names.
   * @throws FijiURIException If the parameters are invalid.
   */
  private CassandraFijiURI(
      final String scheme,
      final Iterable<String> zookeeperQuorum,
      final int zookeeperClientPort,
      final ImmutableList<String> contactPoints,
      final int contactPort,
      final String username,
      final String password,
      final String instanceName,
      final String tableName,
      final Iterable<FijiColumnName> columnNames) {
    super(scheme, zookeeperQuorum, zookeeperClientPort, instanceName, tableName, columnNames);
    mContactPoints = contactPoints;
    mContactPointsNormalized = ImmutableSortedSet.copyOf(mContactPoints).asList();
    mContactPort = contactPort;
    mUsername = username;
    mPassword = password;
  }
  // CSON

  /**
   * Returns the normalized (de-duplicated and sorted) Cassandra contact point hosts.
   *
   * @return the normalized Cassandra contact point hosts.
   */
  public ImmutableList<String> getContactPoints() {
    return mContactPointsNormalized;
  }

  /**
   * Returns the original user-specified list of Cassandra contact point hosts.
   *
   * @return the user-specified Cassandra contact point hosts.
   */
  public ImmutableList<String> getContactPointsOrdered() {
    return mContactPoints;
  }

  /**
   * Returns the Cassandra contact point port.
   *
   * @return the Cassandra contact point port.
   */
  public int getContactPort() {
    return mContactPort;
  }

  /**
   * Returns the Cassandra username.
   *
   * @return the Cassandra username.
   */
  public String getUsername() {
    return mUsername;
  }

  /**
   * Returns the Cassandra password.
   *
   * @return the Cassandra password.
   */
  public String getPassword() {
    return mPassword;
  }

  /**
   * Builder class for constructing CassandraFijiURIs.
   */
  public static final class CassandraFijiURIBuilder extends FijiURIBuilder {

    private ImmutableList<String> mContactPoints;
    private int mContactPort;
    private String mUsername;
    private String mPassword;

    // CSOFF: ParameterNumberCheck

    /**
     * Constructs a new builder for CassandraFijiURIs.
     *
     * @param zookeeperQuorum The initial zookeeper quorum.
     * @param zookeeperClientPort The initial zookeeper client port.
     * @param contactPoints The host names of Cassandra contact points.
     * @param contactPort The port of Cassandra contact points.
     * @param username for Cassandra authentication (can be null)
     * @param password for Cassandra authentication (can be null)
     * @param instanceName The initial instance name.
     * @param tableName The initial table name.
     * @param columnNames The initial column names.
     */

    private CassandraFijiURIBuilder(
        final Iterable<String> zookeeperQuorum,
        final int zookeeperClientPort,
        final Iterable<String> contactPoints,
        final int contactPort,
        final String username,
        final String password,
        final String instanceName,
        final String tableName,
        final Iterable<FijiColumnName> columnNames
    ) {
      super(
          CASSANDRA_SCHEME,
          zookeeperQuorum,
          zookeeperClientPort,
          instanceName,
          tableName,
          columnNames);
      mContactPoints = ImmutableList.copyOf(contactPoints);
      mContactPort = contactPort;
      mUsername = username;
      mPassword = password;
    }
    // CSOFF

    /**
     * Constructs a new builder for CassandraFijiURIs.
     */
    public CassandraFijiURIBuilder() {
      super(CASSANDRA_SCHEME);
      mContactPoints = ImmutableList.of(DEFAULT_CONTACT_POINT);
      mContactPort = DEFAULT_CONTACT_PORT;
    }

    /**
     * Overridden to provide specific return type.
     *
     * {@inheritDoc}
     */
    @Override
    public CassandraFijiURIBuilder withZookeeperQuorum(String[] zookeeperQuorum) {
      super.withZookeeperQuorum(zookeeperQuorum);
      return this;
    }

    /**
     * Overridden to provide specific return type.
     *
     * {@inheritDoc}
     */
    @Override
    public CassandraFijiURIBuilder withZookeeperQuorum(Iterable<String> zookeeperQuorum) {
      super.withZookeeperQuorum(zookeeperQuorum);
      return this;
    }

    /**
     * Overridden to provide specific return type.
     *
     * {@inheritDoc}
     */
    @Override
    public CassandraFijiURIBuilder withZookeeperClientPort(int zookeeperClientPort) {
      super.withZookeeperClientPort(zookeeperClientPort);
      return this;
    }

    /**
     * Provide a list of Cassandra contact points. These contact points will be used to initially
     * connect to Cassandra.
     *
     * @param contactPoints used create initial connection to Cassandra cluster.
     * @return this.
     */
    public CassandraFijiURIBuilder withContactPoints(final Iterable<String> contactPoints) {
      mContactPoints = ImmutableList.copyOf(contactPoints);
      return this;
    }

    /**
     * Provide a port to connect to Cassandra contact points.
     *
     * @param contactPort to use when connecting to Cassandra contact points.
     * @return this.
     */
    public CassandraFijiURIBuilder withContactPort(final int contactPort) {
      mContactPort = contactPort;
      return this;
    }

    /**
     * Overridden to provide specific return type.
     *
     * {@inheritDoc}
     */
    @Override
    public CassandraFijiURIBuilder withInstanceName(String instanceName) {
      super.withInstanceName(instanceName);
      return this;
    }

    /**
     * Overridden to provide specific return type.
     *
     * {@inheritDoc}
     */
    @Override
    public CassandraFijiURIBuilder withTableName(String tableName) {
      super.withTableName(tableName);
      return this;
    }

    /**
     * Overridden to provide specific return type.
     *
     * {@inheritDoc}
     */
    @Override
    public CassandraFijiURIBuilder withColumnNames(Collection<String> columnNames) {
      super.withColumnNames(columnNames);
      return this;
    }

    /**
     * Overridden to provide specific return type.
     *
     * {@inheritDoc}
     */
    @Override
    public CassandraFijiURIBuilder addColumnNames(Collection<FijiColumnName> columnNames) {
      super.addColumnNames(columnNames);
      return this;
    }

    /**
     * Overridden to provide specific return type.
     *
     * {@inheritDoc}
     */
    @Override
    public CassandraFijiURIBuilder addColumnName(FijiColumnName columnName) {
      super.addColumnName(columnName);
      return this;
    }

    /**
     * Overridden to provide specific return type.
     *
     * {@inheritDoc}
     */
    @Override
    public CassandraFijiURIBuilder withColumnNames(Iterable<FijiColumnName> columnNames) {
      super.withColumnNames(columnNames);
      return this;
    }

    /**
     * Builds the configured CassandraFijiURI.
     *
     * @return A CassandraFijiURI.
     * @throws FijiURIException If the CassandraFijiURI was configured improperly.
     */
    @Override
    public CassandraFijiURI build() {
      return new CassandraFijiURI(
          mScheme,
          mZookeeperQuorum,
          mZookeeperClientPort,
          mContactPoints,
          mContactPort,
          mUsername,
          mPassword,
          mInstanceName,
          mTableName,
          mColumnNames);
    }
  }

  /**
   * A {@link FijiURIParser} for {@link CassandraFijiURI}s.
   */
  public static final class CassandraFijiURIParser implements FijiURIParser {
    /** {@inheritDoc} */
    @Override
    public CassandraFijiURIBuilder parse(final URI uri) {
      Preconditions.checkArgument(uri.getScheme().equals(CASSANDRA_SCHEME),
          "CassandraFijiURIParser can not parse a URI with scheme '%s'.", uri.getScheme());


      // Parse the ZooKeeper portion of the authority.
      final ZooKeeperAuthorityParser zooKeeperAuthorityParser
          = ZooKeeperAuthorityParser.getAuthorityParser(uri);

      // Cassandra Fiji URIs aren't strictly legal - the Cassandra "authority" is really the first
      // part of the path.
      final List<String> segments = Splitter.on('/').omitEmptyStrings().splitToList(uri.getPath());
      if (segments.size() < 1) {
        throw new FijiURIException(uri.toString(), "Cassandra contact points must be specified.");
      }
      final String cassandraAuthority = segments.get(0);
      final AuthorityParser cassandraAuthorityParser = AuthorityParser.getAuthorityParser(
          cassandraAuthority,
          uri);

      // We currently support either neither a username nor a password, OR a username and a
      // password, but not a username without a password.
      if (null != cassandraAuthorityParser.getUsername()
          && null == cassandraAuthorityParser.getPassword()) {
        throw new FijiURIException(uri.toString(),
            "Cassandra Fiji URIs do not support a username without a password.");

      }

      final PathParser segmentParser = new PathParser(uri, 1);

      return new CassandraFijiURIBuilder(
          zooKeeperAuthorityParser.getZookeeperQuorum(),
          zooKeeperAuthorityParser.getZookeeperClientPort(),
          cassandraAuthorityParser.getHosts(),
          cassandraAuthorityParser.getHostPort() == null
              ? DEFAULT_CONTACT_PORT : cassandraAuthorityParser.getHostPort(),
          cassandraAuthorityParser.getUsername(),
          cassandraAuthorityParser.getPassword(),
          segmentParser.getInstance(),
          segmentParser.getTable(),
          segmentParser.getColumns());
    }

    @Override
    public String getName() {
      return CASSANDRA_SCHEME;
    }
  }

  /** {@inheritDoc} */
  @Override
  protected StringBuilder appendClusterIdentifier(
      final StringBuilder sb,
      final boolean preserveOrdering
  ) {
    super.appendClusterIdentifier(sb, preserveOrdering);
    ImmutableList<String> contactPoints =
        preserveOrdering ? mContactPoints : mContactPointsNormalized;

    if (mUsername != null) {
      Preconditions.checkNotNull(mPassword);
      sb.append(mUsername);
      sb.append(":");
      sb.append(mPassword);
      sb.append("@");
    }

    if (contactPoints.size() == 1) {
      sb.append(contactPoints.get(0));
    } else {
      sb.append('(');
      Joiner.on(',').appendTo(sb, contactPoints);
      sb.append(')');
    }
    sb
        .append(':')
        .append(mContactPort)
        .append('/');

    return sb;
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
  public static CassandraFijiURIBuilder newBuilder() {
    return new CassandraFijiURIBuilder();
  }

  /**
   * Gets a builder configured with a Fiji URI.
   *
   * @param uri The Fiji URI to configure the builder from.
   * @return A builder configured with uri.
   */
  public static CassandraFijiURIBuilder newBuilder(FijiURI uri) {
    final ImmutableList<String> contactPoints;
    final int contactPort;
    final String username;
    final String password;
    if (uri instanceof CassandraFijiURI) {
      final CassandraFijiURI cassandraFijiURI = (CassandraFijiURI) uri;
      contactPoints = cassandraFijiURI.mContactPoints;
      contactPort = cassandraFijiURI.mContactPort;
      username = cassandraFijiURI.mUsername;
      password = cassandraFijiURI.mPassword;
    } else {
      contactPoints = ImmutableList.of(DEFAULT_CONTACT_POINT);
      contactPort = DEFAULT_CONTACT_PORT;
      username = null;
      password = null;
    }

    return new CassandraFijiURIBuilder(
        uri.getZookeeperQuorumOrdered(),
        uri.getZookeeperClientPort(),
        contactPoints,
        contactPort,
        username,
        password,
        uri.getInstance(),
        uri.getTable(),
        uri.getColumnsOrdered());
  }

  /**
   * Gets a builder configured with the Fiji URI.
   *
   * <p> The String parameter can be a relative URI (with a specified instance), in which
   *     case it is automatically normalized relative to the default Cassandra URI.
   *
   * @param uri String specification of a Fiji URI.
   * @return A builder configured with uri.
   * @throws FijiURIException If the uri is invalid.
   */
  public static CassandraFijiURIBuilder newBuilder(String uri) {
    if (!(uri.startsWith(CASSANDRA_SCHEME))) {
      uri = String.format("fiji-cassandra://%s/%s:%s/%s",
          FijiURI.ENV_URI_STRING, DEFAULT_CONTACT_POINT, DEFAULT_CONTACT_PORT, uri);
    }
    try {
      return new CassandraFijiURIParser().parse(new URI(uri));
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
  public CassandraFijiURI resolve(String path) {
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
  protected CassandraFijiURIBuilder getBuilder() {
    return newBuilder(this);
  }

  /** {@inheritDoc} */
  @Override
  protected CassandraFijiFactory getFijiFactory() {
    return new CassandraFijiFactory();
  }

  /** {@inheritDoc} */
  @Override
  protected CassandraFijiInstaller getFijiInstaller() {
    return CassandraFijiInstaller.get();
  }
}
