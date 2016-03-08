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
package com.moz.fiji.schema;

import java.io.Closeable;
import java.util.Iterator;

import com.moz.fiji.annotations.ApiAudience;
import com.moz.fiji.annotations.ApiStability;
import com.moz.fiji.annotations.Inheritance;

/**
 * A scanner over rows in a FijiTable. Rows are returned as {@link FijiResult}s.
 * {@code FijiResultScanner} must be closed when it will no longer be used.
 *
 * @param <T> type of {@code FijiCell} value returned by scanned {@code FijiResult}s.
 */
@ApiAudience.Framework
@ApiStability.Experimental
@Inheritance.Sealed
public interface FijiResultScanner<T> extends Closeable, Iterator<FijiResult<T>> { }
