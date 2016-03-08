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

package com.moz.fiji.schema.security;

import java.util.EnumSet;
import java.util.Set;

import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;
import org.apache.hadoop.hbase.security.access.Permission;

import com.moz.fiji.annotations.ApiAudience;
import com.moz.fiji.annotations.ApiStability;

/**
 * Encapsulates the permissions state for a user of a Fiji instance.
 *
 * <p>Instances of FijiPermissions are immutable.  All methods that change the allowed actions
 * return a new instance of FijiPermissions with the new actions.</p>
 *
 * This class may be modified later to also represent the permissions of Fiji tables, columns, or
 * rows.
 */
@ApiAudience.Framework
@ApiStability.Experimental
public final class FijiPermissions {
  /** Actions in Fiji that have an HBase Action counterpart. */
  private static final EnumSet<Action> HBASE_ACTIONS = EnumSet.of(
      Action.READ,
      Action.WRITE,
      Action.GRANT);

  /** A singleton empty permissions object. */
  private static final FijiPermissions EMPTY_PERMISSIONS_SINGLETON =
      new FijiPermissions(EnumSet.noneOf(Action.class));

  /** All actions allowed by this FijiPermissions object. */
  private final Set<Action> mActions;

  /**
   * Constructs a new FijiPermissions object with the actions specified.
   *
   * @param actions to permit.
   */
  private FijiPermissions(Set<Action> actions) {
    mActions = EnumSet.copyOf(actions);
  }

  /**
   * Constructs a new FijiPermissions with no permitted actions.
   *
   * @return a new FijiPermissions objects with no permitted actions.
   */
  public static FijiPermissions emptyPermissions() {
    return EMPTY_PERMISSIONS_SINGLETON;
  }

  /**
   * Constructs a new FijiPermissions with the specified actions.
   *
   * @param actions allowed by the FijiPermissions returned.
   * @return a FijiPermissions with the specified actions.
   */
  public static FijiPermissions newWithActions(Set<Action> actions) {
    return new FijiPermissions(actions);
  }

  /** All possible actions in Fiji. */
  public static enum Action {
    READ(Permission.Action.READ, "fiji.security-0.1.permission.read"),
    WRITE(Permission.Action.WRITE, "fiji.security-0.1.permission.write"),
    GRANT(Permission.Action.ADMIN, "fiji.security-0.1.permission.grant");

    /** The corresponding HBase Permission.Action.  Null if no corresponding HBase Action. */
    private final Permission.Action mHBaseAction;

    /** The string used as an HBase row key used to store users with this Action in Fiji. */
    private final String mStringKey;

    /**
     * Gets the string used as an HBase row key to store users with this Action in Fiji.
     *
     * @return the string used as an HBase row key to store users with this Action in Fiji.
     */
    protected String getStringKey() {
      return mStringKey;
    }

    /**
     * Construct a new Fiji Action.
     *
     * @param hBaseAction The corresponding HBase Permission.Action. Null if no corresponding
     *     HBase Action.
     * @param stringKey The string used as an HBase row key to store users with this Action in
     *     Fiji.
     */
    private Action(Permission.Action hBaseAction, String stringKey) {
      mHBaseAction = hBaseAction;
      mStringKey = stringKey;
    }

    /**
     * Gets the corresponding HBase Permission.Action, or null if there is none.
     *
     * @return The corresponding HBase Permission.Action, or null if there is none.
     */
    private Permission.Action getHBaseAction() {
      return mHBaseAction;
    }
  }

  /**
   * Constructs a new FijiPermissions object with the specified action added to the current
   * permissions.
   *
   * @param action to add.
   * @return a new FijiPermissions with 'action' added.
   */
  public FijiPermissions addAction(Action action) {
    Preconditions.checkNotNull(action);
    Set<Action> newActions = EnumSet.copyOf(mActions);
    newActions.add(action);
    return new FijiPermissions(newActions);
  }

  /**
   * Constructs a new FijiPermissions object with the specified action removed from the current
   * permissions.  If 'action' is not in the current permissions, returns a new FijiPermissions
   * with the same permissions.
   *
   * @param action to remove.
   * @return a new FijiPermissions with 'action' removed, if it was present.
   */
  public FijiPermissions removeAction(Action action) {
    Preconditions.checkNotNull(action);
    Set<Action> newActions = EnumSet.copyOf(mActions);
    newActions.remove(action);
    return new FijiPermissions(newActions);
  }

  /**
   * Gets a copy of the actions allowed by this FijiPermissions.
   *
   * @return the actions allowed by this FijiPermissions.
   */
  public Set<Action> getActions() {
    return EnumSet.copyOf(mActions);
  }

  /**
   * Returns true if this FijiPermissions allows the action, false otherwise.
   *
   * @param action to check permissions for.
   * @return true if this Fiji Permissions allows the action, false otherwise.
   */
  public boolean allowsAction(Action action) {
    Preconditions.checkNotNull(action);
    return mActions.contains(action);
  }

  /**
   * Returns an array of all HBase Actions specified by this FijiPermissions object. This is used
   * internally to apply the correct permissions on HBase tables.
   *
   * @return HBase Actions for all of the permitted Actions with corresponding HBase Actions.
   */
  protected Permission.Action[] toHBaseActions() {
    final Set<Action> mHBaseActions = Sets.intersection(mActions, HBASE_ACTIONS);
    final Set<Permission.Action> convertedActions = EnumSet.noneOf(Permission.Action.class);
    for (Action fijiAction : mHBaseActions) {
      convertedActions.add(fijiAction.getHBaseAction());
    }
    return convertedActions.toArray(new Permission.Action[convertedActions.size()]);
  }
}
