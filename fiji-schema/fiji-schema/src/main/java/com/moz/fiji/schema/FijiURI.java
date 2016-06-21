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

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Collection;
import java.util.regex.Pattern;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSortedSet;

import com.moz.fiji.annotations.ApiAudience;
import com.moz.fiji.annotations.ApiStability;
import com.moz.fiji.schema.hbase.HBaseFijiURI.HBaseFijiURIBuilder;
import com.moz.fiji.schema.impl.FijiURIParser;
import com.moz.fiji.schema.impl.FijiURIParser.ZooKeeperAuthorityParser;
import com.moz.fiji.schema.util.FijiNameValidator;
import com.moz.fiji.schema.zookeeper.ZooKeeperFactory;

/**
 * a {@code URI} that uniquely identifies a Fiji instance, table, and column-set.
 * Use {@value com.moz.fiji.schema.KConstants#DEFAULT_HBASE_URI} for the default Fiji instance URI.
 *
 * <p>
 *
 * {@code FijiURI} objects can be constructed directly from parsing a URI string:
 * <pre><code>
 * final FijiURI uri = FijiURI.newBuilder("fiji://.env/default/mytable/col").build();
 * </code></pre>
 *
 * <p>
 *
 * Alternatively, {@code FijiURI} objects can be constructed from components by using a builder:
 * <pre><code>
 * final FijiURI uri = FijiURI.newBuilder()
 *   .withInstanceName("default")
 *   .withTableName("mytable")
 *   .addColumnName(FijiColumnName.create(col))
 *   .build();
 * </code></pre>
 *
 * <H2>Syntax</H2>
 *
 * A FijiURI is composed of multiple components: a {@code scheme}, a {@code cluster-identifier},
 * and optionally, an {@code instance-name}, a {@code table-name}, and {@code column-names}.
 * The text format of a {@code FijiURI} must be of the form:
 * <pre><code>
 * scheme://cluster-identifier[/instance-name[/table-name[/column-names]]]
 * </code></pre>
 * where square brackets indicate optional components.
 *
 * <H3>Scheme</H3>
 *
 * The scheme of all {@code FijiURI}s is a identifier prefixed with the string "{@code fiji}", and
 * followed by any combination of letters, digits, plus ("+"), period ("."), or hyphen ("-").
 * <p>
 * The scheme is specific to the type of cluster identified, and determines how the remaining
 * components will be parsed.
 * <p>
 * The default {@code FijiURI} scheme is "{@value #FIJI_SCHEME}". When this scheme is parsed an
 * {@link com.moz.fiji.schema.hbase.HBaseFijiURI} will be created.
 *
 * <H3>Cluster Identifier</H3>
 *
 * The cluster identifier contains the information necessary for Fiji to identify the host cluster.
 * The exact form of the cluster identifier is specific to the cluster type (which is identified
 * by the scheme).
 *
 * At a minimum, the cluster identifier includes ZooKeeper ensemble information for connecting to
 * the ZooKeeper service hosting Fiji. The ZooKeeper Ensemble address is located in the 'authority'
 * position as defined in RFC3986, and may take one of the following host/port forms, depending on
 * whether one or more hosts is specified, and whether a port is specified:
 *
 * <ul>
 * <li> {@code .env}</li>
 * <li> {@code host}</li>
 * <li> {@code host:port}</li>
 * <li> {@code host1,host2}</li>
 * <li> {@code (host1,host2):port}</li>
 * </ul>
 *
 * The {@value #ENV_URI_STRING} value will resolve at runtime to a ZooKeeper ensemble address
 * taken from the environment. Specifics of how the address is resolved is scheme-specific.
 * <p>
 * Note that, depending on the scheme, the cluster identifier may contain path segments, and thus
 * is potentially larger than just the URI authority.
 *
 * <H3>Instance Name</H3>
 *
 * The instance name component is optional. Identifies a Fiji instance hosted on the cluster.
 * Only valid Fiji instance names may be used.
 *
 * <H3>Table Name</H3>
 *
 * The table name component is optional, and may only be used if the instance name is defined.
 * The table name identifies a Fiji table in the identified instance. Only a valid Fiji table name
 * may be used.
 *
 * <H3>Column Names</H3>
 *
 * The column names component is optional, and may only be used if the table name is defined.
 * The column names identify a set of Fiji column names in the specified table. The column names
 * are comma separated with no spaces, and may contain only valid Fiji column names.
 *
 * <H2>Examples</H2>
 *
 * The following are valid {@code FijiURI}s:
 *
 * <ul>
 * <li> {@code fiji://zkHost}</li>
 * <li> {@code fiji://zkHost/instance}</li>
 * <li> {@code fiji://zkHost/instance/table}</li>
 * <li> {@code fiji://zkHost:zkPort/instance/table}</li>
 * <li> {@code fiji://zkHost1,zkHost2/instance/table}</li>
 * <li> {@code fiji://(zkHost1,zkHost2):zkPort/instance/table}</li>
 * <li> {@code fiji://zkHost/instance/table/col}</li>
 * <li> {@code fiji://zkHost/instance/table/col1,col2}</li>
 * <li> {@code fiji://.env/instance/table}</li>
 * </ul>
 *
 * <H2>Usage</H2>
 *
 * The {@link FijiURI} class is not directly instantiable (it is effectively {@code abstract}).
 * The builder will instead return a concrete subclass based on the scheme of the provided URI or
 * URI string. If no URI or URI string is provided to create the builder, then the default
 * {@link com.moz.fiji.schema.hbase.HBaseFijiURI} will be assumed.
 *
 * All {@link FijiURI} implementations are immutable and thread-safe.
 */
@ApiAudience.Public
@ApiStability.Stable
public class FijiURI {

  /**
   * Default {@code FijiURI} scheme. When a {@code FijiURI} with this scheme is parsed, the default
   * {@code FijiURI} type ({@link com.moz.fiji.schema.hbase.HBaseFijiURI}) is used.
   */
  public static final String FIJI_SCHEME = "fiji";

  /** String to specify an unset FijiURI field. */
  public static final String UNSET_URI_STRING = ".unset";

  /** String to specify a value through the local environment. */
  public static final String ENV_URI_STRING = ".env";

  /** Default Zookeeper port. */
  public static final int DEFAULT_ZOOKEEPER_CLIENT_PORT = 2181;


  /** Pattern matching "(host1,host2,host3):port". */
  public static final Pattern RE_AUTHORITY_GROUP = Pattern.compile("\\(([^)]+)\\):(\\d+)");

  /** Pattern matching valid Fiji schemes. */
  private static final Pattern RE_SCHEME = Pattern.compile("(?i)fiji[a-z+.-0-9]*://.*");

  /**
   * The scheme of this FijiURI.
   */
  private final String mScheme;

  /**
   * Ordered list of Zookeeper quorum host names or IP addresses.
   * Preserves user ordering. Never null.
   */
  private final ImmutableList<String> mZookeeperQuorum;

  /** Normalized (sorted) version of mZookeeperQuorum. Never null. */
  private final ImmutableList<String> mZookeeperQuorumNormalized;

  /** Zookeeper client port number. */
  private final int mZookeeperClientPort;

  /** Fiji instance name. Null means unset. */
  private final String mInstanceName;

  /** Fiji table name. Null means unset. */
  private final String mTableName;

  /** Fiji column names. Never null. Empty means unset. Preserves user ordering. */
  private final ImmutableList<FijiColumnName> mColumnNames;

  /** Normalized version of mColumnNames. Never null. */
  private final ImmutableList<FijiColumnName> mColumnNamesNormalized;

  /**
   * Constructs a new FijiURI with the given parameters.
   *
   * @param scheme of the FijiURI.
   * @param zookeeperQuorum Zookeeper quorum.
   * @param zookeeperClientPort Zookeeper client port.
   * @param instanceName Instance name.
   * @param tableName Table name.
   * @param columnNames Column names.
   * @throws FijiURIException If the parameters are invalid.
   */
  protected FijiURI(
      final String scheme,
      final Iterable<String> zookeeperQuorum,
      final int zookeeperClientPort,
      final String instanceName,
      final String tableName,
      final Iterable<FijiColumnName> columnNames
  ) {
    mScheme = scheme;
    mZookeeperQuorum = ImmutableList.copyOf(zookeeperQuorum);
    mZookeeperQuorumNormalized = ImmutableSortedSet.copyOf(mZookeeperQuorum).asList();
    mZookeeperClientPort = zookeeperClientPort;
    mInstanceName =
        ((null == instanceName) || !instanceName.equals(UNSET_URI_STRING)) ? instanceName : null;
    mTableName = ((null == tableName) || !tableName.equals(UNSET_URI_STRING)) ? tableName : null;
    mColumnNames = ImmutableList.copyOf(columnNames);
    mColumnNamesNormalized = ImmutableSortedSet.copyOf(mColumnNames).asList();
    validateNames();
  }

  /**
   * Builder class for constructing FijiURIs.
   */
  public static class FijiURIBuilder {

    /**
     * The scheme of the FijiURI being built.
     */
    protected String mScheme;

    /**
     * Zookeeper quorum: comma-separated list of Zookeeper host names or IP addresses.
     * Preserves user ordering.
     */
    protected ImmutableList<String> mZookeeperQuorum;

    /** Zookeeper client port number. */
    protected int mZookeeperClientPort;

    /** Fiji instance name. Null means unset. */
    protected String mInstanceName;

    /** Fiji table name. Null means unset. */
    protected String mTableName;

    /** Fiji column names. Never null. Empty means unset. Preserves user ordering. */
    protected ImmutableList<FijiColumnName> mColumnNames;

    /**
     * Constructs a new builder for FijiURIs.
     *
     * @param scheme of the URI.
     * @param zookeeperQuorum The initial zookeeper quorum.
     * @param zookeeperClientPort The initial zookeeper client port.
     * @param instanceName The initial instance name.
     * @param tableName The initial table name.
     * @param columnNames The initial column names.
     */
    protected FijiURIBuilder(
        final String scheme,
        final Iterable<String> zookeeperQuorum,
        final int zookeeperClientPort,
        final String instanceName,
        final String tableName,
        final Iterable<FijiColumnName> columnNames) {
      mScheme = scheme;
      mZookeeperQuorum = ImmutableList.copyOf(zookeeperQuorum);
      mZookeeperClientPort = zookeeperClientPort;
      mInstanceName =
          ((null == instanceName) || !instanceName.equals(UNSET_URI_STRING)) ? instanceName : null;
      mTableName = ((null == tableName) || !tableName.equals(UNSET_URI_STRING)) ? tableName : null;
      mColumnNames = ImmutableList.copyOf(columnNames);
    }

    /**
     * Constructs a new builder for FijiURIs with default values.
     * See {@link FijiURI#newBuilder()} for specific values.
     */
    protected FijiURIBuilder() {
      this(FIJI_SCHEME);
    }

    /**
     * Constructs a new builder for FijiURIs with default values and the given scheme.
     * See {@link FijiURI#newBuilder()} for specific values.
     *
     * @param scheme The scheme to use for the built Fiji URI.
     */
    protected FijiURIBuilder(final String scheme) {
      mScheme = scheme;
      mZookeeperQuorum = ZooKeeperAuthorityParser.ENV_ZOOKEEPER_QUORUM;
      mZookeeperClientPort = ZooKeeperAuthorityParser.ENV_ZOOKEEPER_CLIENT_PORT;
      mInstanceName = KConstants.DEFAULT_INSTANCE_NAME;
      mTableName = UNSET_URI_STRING;
      mColumnNames = ImmutableList.of();
    }

    /**
     * Configures the FijiURI with Zookeeper Quorum.
     *
     * @param zookeeperQuorum The zookeeper quorum.
     * @return This builder instance so you may chain configuration method calls.
     */
    public FijiURIBuilder withZookeeperQuorum(String[] zookeeperQuorum) {
      mZookeeperQuorum = ImmutableList.copyOf(zookeeperQuorum);
      return this;
    }

    /**
     * Configures the FijiURI with Zookeeper Quorum.
     *
     * @param zookeeperQuorum The zookeeper quorum.
     * @return This builder instance so you may chain configuration method calls.
     */
    public FijiURIBuilder withZookeeperQuorum(Iterable<String> zookeeperQuorum) {
      mZookeeperQuorum = ImmutableList.copyOf(zookeeperQuorum);
      return this;
    }

    /**
     * Configures the FijiURI with the Zookeeper client port.
     *
     * @param zookeeperClientPort The port.
     * @return This builder instance so you may chain configuration method calls.
     */
    public FijiURIBuilder withZookeeperClientPort(int zookeeperClientPort) {
      mZookeeperClientPort = zookeeperClientPort;
      return this;
    }

    /**
     * Configures the FijiURI with the Fiji instance name.
     *
     * @param instanceName The Fiji instance name.
     * @return This builder instance so you may chain configuration method calls.
     */
    public FijiURIBuilder withInstanceName(String instanceName) {
      mInstanceName = instanceName;
      return this;
    }

    /**
     * Configures the FijiURI with the Fiji table name.
     *
     * @param tableName The Fiji table name.
     * @return This builder instance so you may chain configuration method calls.
     */
    public FijiURIBuilder withTableName(String tableName) {
      mTableName = tableName;
      return this;
    }

    /**
     * Configures the FijiURI with the Fiji column names.
     *
     * @param columnNames The Fiji column names to configure.
     * @return This builder instance so you may chain configuration method calls.
     */
    public FijiURIBuilder withColumnNames(Collection<String> columnNames) {
      ImmutableList.Builder<FijiColumnName> builder = ImmutableList.builder();
      for (String column : columnNames) {
        builder.add(FijiColumnName.create(column));
      }
      mColumnNames = builder.build();
      return this;
    }

    /**
     * Adds the column names to the Fiji URI column names.
     *
     * @param columnNames The Fiji column names to add.
     * @return This builder instance so you may chain configuration method calls.
     */
    public FijiURIBuilder addColumnNames(Collection<FijiColumnName> columnNames) {
      ImmutableList.Builder<FijiColumnName> builder = ImmutableList.builder();
      builder.addAll(mColumnNames).addAll(columnNames);
      mColumnNames = builder.build();
      return this;
    }

    /**
     * Adds the column name to the Fiji URI column names.
     *
     * @param columnName The Fiji column name to add.
     * @return This builder instance so you may chain configuration method calls.
     */
    public FijiURIBuilder addColumnName(FijiColumnName columnName) {
      ImmutableList.Builder<FijiColumnName> builder = ImmutableList.builder();
      builder.addAll(mColumnNames).add(columnName);
      mColumnNames = builder.build();
      return this;
    }

    /**
     * Configures the FijiURI with the Fiji column names.
     *
     * @param columnNames The Fiji column names.
     * @return This builder instance so you may chain configuration method calls.
     */
    public FijiURIBuilder withColumnNames(Iterable<FijiColumnName> columnNames) {
      mColumnNames = ImmutableList.copyOf(columnNames);
      return this;
    }

    /**
     * Builds the configured FijiURI.
     *
     * @return A FijiURI.
     * @throws FijiURIException If the FijiURI was configured improperly.
     */
    public FijiURI build() {
      throw new UnsupportedOperationException("Abstract method.");
    }
  }

  /**
   * Gets a builder configured with default Fiji URI fields for an HBase cluster.
   *
   * More precisely, the following defaults are initialized:
   * <ul>
   *   <li>The Zookeeper quorum and client port is taken from the Hadoop <tt>Configuration</tt></li>
   *   <li>The Fiji instance name is set to <tt>KConstants.DEFAULT_INSTANCE_NAME</tt>
   *       (<tt>"default"</tt>).</li>
   *   <li>The table name and column names are explicitly left unset.</li>
   * </ul>
   *
   * @return A builder configured with defaults for an HBase cluster.
   */
  public static FijiURIBuilder newBuilder() {
    return new HBaseFijiURIBuilder();
  }

  /**
   * Gets a builder configured with a Fiji URI.
   *
   * @param uri The Fiji URI to configure the builder from.
   * @return A builder configured with uri.
   */
  public static FijiURIBuilder newBuilder(FijiURI uri) {
    return uri.getBuilder();
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
  public static FijiURIBuilder newBuilder(final String uri) {
    final String uriWithScheme;
    if (RE_SCHEME.matcher(uri).matches()) {
      uriWithScheme = uri;
    } else {
      uriWithScheme = String.format("%s/%s/", KConstants.DEFAULT_HBASE_URI, uri);
    }
    try {
      return FijiURIParser.Factory.get(new URI(uriWithScheme));
    } catch (URISyntaxException exn) {
      throw new FijiURIException(uri, exn.getMessage());
    }
  }

  /**
   * Resolve the path relative to this FijiURI. Returns a new instance.
   *
   * @param path The path to resolve.
   * @return The resolved FijiURI.
   * @throws FijiURIException If this FijiURI is malformed.
   */
  public FijiURI resolve(String path) {
    try {
      // Without the "./", URI will assume a path containing a colon
      // is a new URI, for example "family:column".
      URI uri = new URI(toString()).resolve(String.format("./%s", path));
      return FijiURIParser.Factory.get(uri).build();
    } catch (URISyntaxException e) {
      throw new RuntimeException(
          String.format("FijiURI was incorrectly constructed (should never happen): %s",
              this.toString()));
    } catch (IllegalArgumentException e) {
      throw new FijiURIException(this.toString(),
          String.format("Path can not be resolved: %s", path));
    }
  }

  /**
   * Returns the address of each member of the ZooKeeper ensemble associated with this FijiURI
   * in comma-separated host:port  (standard ZooKeeper) format. This method will always return the
   * correct addresses of the ZooKeeper ensemble which hosts the metadata for this FijiURI's
   * instance.
   *
   * @return the addresses of the ZooKeeper ensemble members of the Fiji cluster.
   */
  public String getZooKeeperEnsemble() {
    return ZooKeeperFactory.Provider.get().getZooKeeperEnsemble(this);
  }

  /**
   * Returns the set of Zookeeper quorum hosts (names or IPs).
   *
   * <p> This method is not always guaranteed to return valid ZooKeeper hostnames, instead use
   *    {@link com.moz.fiji.schema.FijiURI#getZooKeeperEnsemble()}. </p>
   *
   * <p> Host names or IP addresses are de-duplicated and sorted. </p>
   *
   * @return the set of Zookeeper quorum hosts (names or IPs).
   *     Never null.
   */
  public ImmutableList<String> getZookeeperQuorum() {
    return mZookeeperQuorumNormalized;
  }

  /**
   * Returns the original user-specified list of Zookeeper quorum hosts.
   *
   * <p> This method is not always guaranteed to return valid ZooKeeper hostnames, instead use
   *    {@link com.moz.fiji.schema.FijiURI#getZooKeeperEnsemble()}. </p>
   *
   * <p> Host names are exactly as specified by the user. </p>
   *
   * @return the original user-specified list of Zookeeper quorum hosts.
   *     Never null.
   */
  public ImmutableList<String> getZookeeperQuorumOrdered() {
    return mZookeeperQuorum;
  }

  /** @return Zookeeper client port. */
  public int getZookeeperClientPort() {
    return mZookeeperClientPort;
  }

  /**
   * Returns the scheme of this FijiURI.
   *
   * @return the scheme of this FijiURI.
   */
  public String getScheme() {
    return mScheme;
  }

  /**
   * Returns the name of the Fiji instance specified by this URI, if any.
   *
   * @return the name of the Fiji instance specified by this URI.
   *     Null means unspecified (ie. this URI does not target a Fiji instance).
   */
  public String getInstance() {
    return mInstanceName;
  }

  /**
   * Returns the name of the Fiji table specified by this URI, if any.
   *
   * @return the name of the Fiji table specified by this URI.
   *     Null means unspecified (ie. this URI does not target a Fiji table).
   */
  public String getTable() {
    return mTableName;
  }

  /** @return Fiji columns (comma-separated list of Fiji column names), normalized. Never null. */
  public ImmutableList<FijiColumnName> getColumns() {
    return mColumnNamesNormalized;
  }

  /** @return Fiji columns (comma-separated list of Fiji column names), ordered. Never null. */
  public Collection<FijiColumnName> getColumnsOrdered() {
    return mColumnNames;
  }

  /** {@inheritDoc} */
  @Override
  public String toString() {
    return toString(false);
  }

  /**
   * Returns a string representation of this URI that preserves ordering of lists in fields,
   * such as the Zookeeper quorum and Fiji columns.
   *
   * @return An order-preserving string representation of this URI.
   */
  public String toOrderedString() {
    return toString(true);
  }

  /**
   * Returns a string representation of this URI.
   *
   * @param preserveOrdering Whether to preserve ordering of lsits in fields.
   * @return A string representation of this URI.
   */
  private String toString(boolean preserveOrdering) {
    // Remove trailing unset fields.
    if (!mColumnNames.isEmpty()) {
      return toStringCol(preserveOrdering);
    } else if (mTableName != null) {
      return toStringTable(preserveOrdering);
    } else if (mInstanceName != null) {
      return toStringInstance(preserveOrdering);
    } else {
      return toStringAuthority(preserveOrdering);
    }
  }

  /** {@inheritDoc} */
  @Override
  public int hashCode() {
    return toString().hashCode();
  }

  /**
   * {@inheritDoc}
   *
   * *note* {@code FijiURI}'s are compared based on the string representation of the {@code URI}.
   * If two {@code FijiURI}s point to the same cluster with a different scheme, then they will
   * compare differently, for example {@code fiji://localhost} and {@code fiji-hbase://localhost}.
   */
  @Override
  public boolean equals(Object object) {
    if (object == null) {
      return false;
    }
    return object.getClass() == this.getClass() && object.toString().equals(this.toString());
  }

  /**
   * Validates the names used in the URI.
   *
   * @throws FijiURIException if there is an invalid name in this URI.
   */
  private void validateNames() {
    if ((mInstanceName != null) && !FijiNameValidator.isValidFijiName(mInstanceName)) {
      throw new FijiURIException(String.format(
          "Invalid Fiji URI: '%s' is not a valid Fiji instance name.", mInstanceName));
    }
    if ((mTableName != null) && !FijiNameValidator.isValidLayoutName(mTableName)) {
      throw new FijiURIException(String.format(
          "Invalid Fiji URI: '%s' is not a valid Fiji table name.", mTableName));
    }
  }

  /**
   * Appends the cluster identifier for this {@code FijiURI} to the passed in {@link StringBuilder}.
   *
   * This is a default implementation which will append the ZooKeeper ensemble to the string
   * builder.  May be overridden by subclasses if more than the ZooKeeper ensemble is necessary for
   * the cluster identifier.
   *
   * @param sb a StringBuilder to append the cluster identifier to.
   * @param preserveOrdering whether to preserve ordering in the cluster identifier components.
   * @return the StringBuilder with the cluster identifier appended.
   */
  protected StringBuilder appendClusterIdentifier(
      final StringBuilder sb,
      final boolean preserveOrdering
  ) {
    ImmutableList<String> zookeeperQuorum =
        preserveOrdering ? mZookeeperQuorum : mZookeeperQuorumNormalized;
    if (zookeeperQuorum == null) {
      sb.append(UNSET_URI_STRING);
    } else if (zookeeperQuorum.size() == 1) {
      sb.append(zookeeperQuorum.get(0));
    } else {
      sb.append('(');
      Joiner.on(',').appendTo(sb, zookeeperQuorum);
      sb.append(')');
    }
    sb
      .append(':')
      .append(mZookeeperClientPort)
      .append('/');

    return sb;
  }

  /**
   * Formats the full FijiURI up to the authority, preserving order.
   *
   * @param preserveOrdering Whether to preserve ordering.
   * @return Representation of this FijiURI up to the authority.
   */
  private String toStringAuthority(boolean preserveOrdering) {
    StringBuilder sb = new StringBuilder();
    sb.append(mScheme)
      .append("://");

    appendClusterIdentifier(sb, preserveOrdering);
    return sb.toString();
  }

  /**
   * Formats the full FijiURI up to the instance.
   *
   * @param preserveOrdering Whether to preserve ordering.
   * @return Representation of this FijiURI up to the instance.
   */
  private String toStringInstance(boolean preserveOrdering) {
    return String.format("%s%s/",
        toStringAuthority(preserveOrdering),
        (null == mInstanceName) ? UNSET_URI_STRING : mInstanceName);
  }

  /**
   * Formats the full FijiURI up to the table.
   *
   * @param preserveOrdering Whether to preserve ordering.
   * @return Representation of this FijiURI up to the table.
   */
  private String toStringTable(boolean preserveOrdering) {
    return String.format("%s%s/",
        toStringInstance(preserveOrdering),
        (null == mTableName) ? UNSET_URI_STRING : mTableName);
  }

  /**
   * Formats the full FijiURI up to the column.
   *
   * @param preserveOrdering Whether to preserve ordering.
   * @return Representation of this FijiURI up to the table.
   */
  private String toStringCol(boolean preserveOrdering) {
    String columnField;
    ImmutableList<FijiColumnName> columns =
        preserveOrdering ? mColumnNames : mColumnNamesNormalized;
    if (columns.isEmpty()) {
      columnField = UNSET_URI_STRING;
    } else {
      ImmutableList.Builder<String> builder = ImmutableList.builder();
      for (FijiColumnName column : columns) {
        builder.add(column.getName());
      }
      ImmutableList<String> strColumns = builder.build();
      if (strColumns.size() == 1) {
        columnField = strColumns.get(0);
      } else {
        columnField = Joiner.on(",").join(strColumns);
      }
    }

    try {
      // SCHEMA-6. URI Encode column names using RFC-2396.
      final URI columnsEncoded = new URI(FIJI_SCHEME, columnField, null);
      return String.format("%s%s/",
          toStringTable(preserveOrdering),
          columnsEncoded.getRawSchemeSpecificPart());
    } catch (URISyntaxException e) {
      throw new FijiURIException(e.getMessage());
    }
  }

  /**
   * Creates a builder with fields from this {@code FijiURI}.
   *
   * @return a builder with fields from this {@code FijiURI}.
   */
  protected FijiURIBuilder getBuilder() {
    throw new UnsupportedOperationException("Abstract method.");
  }

  /**
   * Returns a {@link FijiFactory} suitable for installing an instance of this {@code FijiURI}.
   *
   * {@code FijiURI} implementations are required to override this method to return a concrete
   * implementation of {@code FijiFactory} appropriate for the cluster type.
   *
   * @return a {@code FijiFactory} for this {@code FijiURI}.
   */
  protected FijiFactory getFijiFactory() {
    throw new UnsupportedOperationException("Abstract method.");
  }

  /**
   * Returns a {@link FijiFactory} suitable for installing an instance of this {@code FijiURI}.
   *
   * {@code FijiURI} implementations are required to override this method. Furthermore, the
   * overridden implementation is required to return a subclass of {@code FijiInstaller} which
   * has overridden implementations of
   * {@link FijiInstaller#install(FijiURI, com.moz.fiji.schema.hbase.HBaseFactory, java.util.Map,
   * org.apache.hadoop.conf.Configuration)} and
   * {@link FijiInstaller#uninstall(FijiURI, com.moz.fiji.schema.hbase.HBaseFactory,
   * org.apache.hadoop.conf.Configuration)}.
   *
   * @return a {@code FijiFactory} for this {@code FijiURI}.
   */
  protected FijiInstaller getFijiInstaller() {
    throw new UnsupportedOperationException("Abstract method.");
  }
}
