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

package com.moz.fiji.annotations;

import java.lang.annotation.Documented;

/**
 * Annotations that inform users of a package, class or method's intended audience.
 * The audience may be {@link Public}, {@link Framework}, or {@link Private}.
 *
 * <ul>
 *   <li>By default, unlabeled classes should be assumed to be private.</li>
 *   <li>External applications must only use classes marked as {@link Public}.
 *     Public classes will evolve in compatible ways only, subject to {@link ApiStability}
 *     concerns. External client apps that depend on non-public APIs may find that they
 *     have difficulty upgrading Fiji versions.</li>
 *   <li>The {@link Framework} publicity level is reserved for APIs that allow
 *     communication between Fiji modules (e.g., between FijiSchema and fiji-schema-shell).
 *     <p>These are not intended for direct use by clients. Authors
 *     of system-level tools may use these APIs, but should keep a close eye
 *     on dev@fiji.org and issues@fiji.org.</p>
 *
 *     <p>Framework-public APIs <b>may change in incompatible ways</b> between
 *     minor releases (1.1.0 -&gt; 1.2.0) of a component.</p></li>
 *   <li>The {@link Private} publicity level is used for code whose audience is
 *     restricted to the current module (e.g., within FijiSchema). This code may
 *     change in incompatible ways at any time.</li>
 * </ul>
 */
@ApiAudience.Framework
@ApiStability.Evolving
public final class ApiAudience {

  /** This class cannot be constructed. */
  private ApiAudience() { }


  /** An API intended to be used by clients at large. */
  @Documented public @interface Public { }

  /**
   * An API intended to be used between Fiji framework modules.
   */
  @Documented public @interface Framework { }

  /**
   * Only to be used within a Fiji module. The API or semantics of the annotated
   * class may change at any time.
   */
  @Documented public @interface Private { }
}
