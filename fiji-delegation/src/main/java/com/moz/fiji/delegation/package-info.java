/**
 * (c) Copyright 2013 WibiData, Inc.
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

/**
 * A lightweight service loading library based on java.util.ServiceLoader that
 * can be used by Fiji framework modules for dynamically loading different
 * service provider implementations at runtime. This delegates functionality
 * required by "upstream" modules to downstream runtime implementations.
 *
 * <p>The main client class is {@link com.moz.fiji.delegation.Lookup Lookup},
 * which provides an API for lookups, as well as factory methods for
 * lookup service implementations.</p>
 *
 * <p>Clients cannot instantiate a <tt>Lookup</tt> object directly. Instead, use the
 * {@link com.moz.fiji.delegation.Lookups Lookups} factory class.</p>
 *
 * <p>Several more specialized forms of <tt>Lookup</tt> can be used in conjunction
 * with associated "provider" APIs. For instance, {@link com.moz.fiji.delegation.PriorityLookup
 * PriorityLookup} allows the client to choose an implementation based on its relative
 * priority to other implementations; these must implement {@link
 * com.moz.fiji.delegation.PriorityProvider PriorityProvider}.</p>
 */
package com.moz.fiji.delegation;

