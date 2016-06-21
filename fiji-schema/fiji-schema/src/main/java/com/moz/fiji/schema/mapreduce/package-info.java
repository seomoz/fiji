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
 * Fiji MapReduce utilities.
 *
 * <p>This package provides support for building MapReduce jobs that read from and/or write to
 * a Fiji table. For reading from Fiji, use {@link
 * com.moz.fiji.schema.mapreduce.FijiTableInputFormat}.</p>
 *
 * <p>December 20, 2012: Note that this package is deprecated; the FijiMapReduce framework
 * (https://github.com/fijiproject/fiji-mapreduce) will contain revamped versions of these
 * and additional MapReduce integration points. Users investing heavily in MapReduce
 * should pay attention to development on this new project at https://jira.fiji.org and the
 * user and developer mailing lists user@ and dev@fiji.org. After FijiMapReduce is released,
 * this package will disappear.</p>
 */
@Deprecated
package com.moz.fiji.schema.mapreduce;
