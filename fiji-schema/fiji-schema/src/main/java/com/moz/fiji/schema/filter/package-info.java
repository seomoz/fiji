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

/**
 * Row level filters used when reading data from Fiji tables.
 *
 * <p>
 * Provides row level filters which are applied to data read from
 * {@link com.moz.fiji.schema.FijiTable FijiTable}. Only one filter can be used
 * at a time, but a single filter can be composed as a
 * combination of multiple filters using the operator filters:
 * {@link com.moz.fiji.schema.filter.AndRowFilter AndRowFilter} and
 * {@link com.moz.fiji.schema.filter.OrRowFilter OrRowFilter}.  Filters are applied to
 * {@link com.moz.fiji.schema.FijiDataRequest FijiDataRequest} objects.
 * </p>
 */
package com.moz.fiji.schema.filter;
