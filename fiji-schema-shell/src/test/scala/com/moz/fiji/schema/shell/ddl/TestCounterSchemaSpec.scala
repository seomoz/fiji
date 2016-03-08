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

package com.moz.fiji.schema.shell.ddl

import scala.collection.JavaConversions._
import org.specs2.mutable._

class TestCounterSchemaSpec extends CommandTestCase {
  "CounterSchemaSpec" should {
    "have a null value" in {
      val counterSpec = new CounterSchemaSpec
      // For now, ok to provide null tableContext to this method; current implementation
      // of CounterSchemaSpec.toNewCellSchema() doesn't actually use it.
      // This may change in the future.
      val cellSchema = counterSpec.toNewCellSchema(null)

      cellSchema.getValue() must beNull
    }
  }
}
