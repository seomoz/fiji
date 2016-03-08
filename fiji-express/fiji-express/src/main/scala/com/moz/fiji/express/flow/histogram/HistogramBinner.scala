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

package com.moz.fiji.express.flow.histogram

import com.moz.fiji.annotations.ApiAudience
import com.moz.fiji.annotations.ApiStability
import com.moz.fiji.annotations.Inheritance

/**
 * Intended to be implemented to provide binning behavior for a histogram.
 */
@ApiAudience.Framework
@ApiStability.Experimental
@Inheritance.Extensible
trait HistogramBinner {
  /**
   * Calculates the bin that a value should affect.
   *
   * @param value to bin.
   * @return the id number of the affected bin.
   */
  def binValue(value: Double): Int

  /**
   * Calculates the lower bound of a bin.
   *
   * @param binId is the id of the bin to calculate the lower bound for.
   * @return the lower bound of a bin.
   */
  def binBoundary(binId: Int): Double
}
