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

import java.util.HashSet;
import java.util.Set;

import org.junit.Test;

/** Unit tests for FijiPermissions. */
public class TestFijiPermissions {
  @Test
  public void testCreateEmpty() {
    FijiPermissions permissions = FijiPermissions.emptyPermissions();

    assert(0 == permissions.getActions().size());
    // No actions should be allowed
    for (FijiPermissions.Action action : FijiPermissions.Action.values()) {
      assert(!permissions.allowsAction(action));
    }
  }

  @Test
  public void testAdd() {
    FijiPermissions emptyPermissions = FijiPermissions.emptyPermissions();
    FijiPermissions grantPermissions = emptyPermissions.addAction(FijiPermissions.Action.GRANT);
    assert(grantPermissions.allowsAction(FijiPermissions.Action.GRANT));
  }

  @Test
  public void testCreateAndRemove() {
    Set<FijiPermissions.Action> actions = new HashSet<FijiPermissions.Action>();
    actions.add(FijiPermissions.Action.READ);
    actions.add(FijiPermissions.Action.WRITE);

    FijiPermissions readWritePermissions = FijiPermissions.newWithActions(actions);
    assert(readWritePermissions.allowsAction(FijiPermissions.Action.READ));
    assert(readWritePermissions.allowsAction(FijiPermissions.Action.WRITE));

    FijiPermissions readOnlyPermissions =
        readWritePermissions.removeAction(FijiPermissions.Action.WRITE);
    assert(readOnlyPermissions.allowsAction(FijiPermissions.Action.READ));
    assert(!readOnlyPermissions.allowsAction(FijiPermissions.Action.WRITE));
  }
}
