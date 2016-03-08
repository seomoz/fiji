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

package com.moz.fiji.express.flow.util

import com.moz.fiji.schema.util.InstanceBuilder
import org.junit.Assert
import org.junit.Test
import com.moz.fiji.express.flow.ColumnInputSpec
import com.moz.fiji.schema.layout.FijiTableLayouts
import com.moz.fiji.express.FijiSuite

class ColumnSpecUtilSuite extends FijiSuite {

  @Test
  def testQualifiedColumnInputSpec(): Unit = {
    try {
      try {
        val fiji = new InstanceBuilder()
            .withTable(FijiTableLayouts.getLayout(FijiTableLayouts.SIMPLE_TWO_COLUMNS))
            .build()

        val table = fiji.openTable("table")

        val specs = ColumnSpecUtil.columnSpecsFromURI(table.getURI.toString)
        assert(specs.size == 2)
        specs.foreach { spec: (ColumnInputSpec, Symbol) =>
            spec._2 match {
              case Symbol("familyColumn1") =>
                Assert.assertEquals("family", spec._1.columnName.getFamily)
                Assert.assertEquals("column1", spec._1.columnName.getQualifier)
              case Symbol("familyColumn2") =>
                Assert.assertEquals("family", spec._1.columnName.getFamily)
                Assert.assertEquals("column2", spec._1.columnName.getQualifier)
              case _ => fail()
            }
        }

        table.release()
        fiji.release()
      }
    }
  }

  @Test
  def testMapFamilyInputSpec(): Unit = {
    try {
      try {
        val fiji = new InstanceBuilder()
            .withTable(FijiTableLayouts.getLayout(FijiTableLayouts.GATHER_MAP_TEST))
            .build()

        val table = fiji.openTable("user")

        val specs = ColumnSpecUtil.columnSpecsFromURI(table.getURI.toString)
        assert(specs.size == 2)
        specs.foreach { spec: (ColumnInputSpec, Symbol) =>
          spec._2 match {
            case Symbol("infoName") =>
              Assert.assertEquals("info", spec._1.columnName.getFamily)
              Assert.assertEquals("name", spec._1.columnName.getQualifier)
            case Symbol("purchases") =>
              Assert.assertEquals("purchases", spec._1.columnName.getFamily)
            case _ => fail()
          }
        }

        table.release()
        fiji.release()
      }
    }
  }
}
