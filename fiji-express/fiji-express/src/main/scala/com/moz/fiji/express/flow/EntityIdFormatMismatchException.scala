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

package com.moz.fiji.express.flow

import com.moz.fiji.annotations.ApiAudience
import com.moz.fiji.annotations.ApiStability
import com.moz.fiji.annotations.Inheritance

/**
 * A runtime exception thrown when two EntityIds with different formats are compared.
 *
 * @param thisComponents components of one of the EntityIds that is mismatched.
 * @param thatComponents components of the other of the EntityIds that is mismatched.
 */
@ApiAudience.Public
@ApiStability.Stable
@Inheritance.Sealed
class EntityIdFormatMismatchException(thisComponents: Seq[Any],thatComponents: Seq[Any])
    extends RuntimeException("Mismatched Formats: %s and  %s do not match.".format(
        "Components: [%s]".format(thisComponents.map { _.getClass.getName }.mkString(",")),
        "Components: [%s]".format(thatComponents.map { _.getClass.getName }.mkString(","))))
