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

package com.moz.fiji.mapreduce;

import org.apache.hadoop.mapreduce.Reducer;

import com.moz.fiji.annotations.ApiAudience;
import com.moz.fiji.annotations.ApiStability;
import com.moz.fiji.annotations.Inheritance;

/**
 * Base class for Fiji reducer.
 *
 * @param <INKEY> The type of the input key to the mapper.
 * @param <INVALUE> The type of the input value to the mapper.
 * @param <OUTKEY> The type of the output key from the mapper.
 * @param <OUTVALUE> The type of the output value from the mapper.
 */
@ApiAudience.Public
@ApiStability.Stable
@Inheritance.Extensible
public abstract class FijiReducer<INKEY, INVALUE, OUTKEY, OUTVALUE>
    extends Reducer<INKEY, INVALUE, OUTKEY, OUTVALUE>
    implements KVOutputJob {
}
