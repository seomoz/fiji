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

package ${package}

import com.twitter.scalding.Args

import com.moz.fiji.express._
import com.moz.fiji.express.flow._

/**
 * A demonstration of the Fiji API.
 *
 * Reads in data from a table and writes that data to another column.
 */
class DemoFiji(args: Args) extends FijiJob(args) {
  val tableUri: String = args("table")

  FijiInput.builder
      .withTableURI(tableUri)
      .withColumns("info:name" -> 'name)
      .build
      // A no-op read/write for example purposes.
      .map('name -> 'nameCopy) { slice: Seq[FlowCell[CharSequence]] =>
        slice.head.datum.toString
      }
      // Write the length of each post to the specified table.
      .write(FijiOutput.builder
          .withTableURI(tableUri)
          .withColumns('nameCopy -> "info:nameCopy")
          .build)
}
