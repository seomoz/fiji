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

package com.moz.fiji.modeling.examples.AssociationRuleMining

import com.moz.fiji.express.flow._
import com.moz.fiji.modeling.framework.FijiModelingJob
import com.twitter.scalding.{Args, FieldConversions}
import scala.Some

/**
 * To use this class as part of the association rule mining demo, run the following command once
 * your tables are loaded:
 *
 *     express job target/fiji-modeling-examples-{project.version}.jar  \
 *         com.moz.fiji.modeling.examples.AssociationRuleMining.RuleMiner \
 *         --input-table fiji://.env/default/sales --input-column purchased_items \
 *         --output-table fiji://.env/default/product --output-column frequent_itemset_recos
 *
 * To inspect the output:
 *
 *     fiji scan fiji://.env/default/product/frequent_itemset_recos --max-rows=3
 */
class RuleMiner(args: Args) extends FijiModelingJob(args) with FieldConversions {
  val inputTableUri: String = args("input-table")
  val inputColumn: String = args("input-column")
  val outputTableUri: String = args("output-table")
  val outputColumn: String = args("output-column")
  val minBagSize: Int = args.getOrElse("min-bag-size", "2").toInt
  val maxBagSize: Int = args.getOrElse("max-bag-size", "2").toInt
  val supportThreshold: Double = args.getOrElse("support", "0.0001").toDouble

  val totalOrders = FijiInput.builder
      .withTableURI(inputTableUri)
      .withColumns(inputColumn -> 'slice)
      .build
      .groupAll { _.size('norm)}

  FijiInput.builder
      .withTableURI(inputTableUri)
      .withColumns(inputColumn -> 'order)
      .build
      // Convert the input data in the Fiji table into a form that is required by prepareItemSets.
      .map('order -> 'order) {
        order: Seq[FlowCell[String]] => order.map { _.qualifier }.toList
      }
      // Generic frequent itemset mining steps.
      .prepareItemSets[String]('order -> 'itemset, minBagSize, maxBagSize)
      .support('itemset -> 'support, Some(totalOrders), None, 'norm)
      .filter('support) { support: Double => support >= supportThreshold }
      // Convert support to the datatype required by the table.
      .map('support -> 'support) { support: Double => support.toString }
      // Pivot the frequent itemsets into per product recommendations.
      // This step is required any time you would like to convert frequent
      // itemsets into per-product recommendations.
      .flatMap('itemset -> ('entityId, 'recommendation)) {
        // For every frequent itemset {a, b} pivot it into rules of the form "recommend a when you
        // see b" and "recommend b when you see a".
        itemset: String => {
          val products = itemset.split(",")
          List((EntityId(products(0)), products(1)), (EntityId(products(1)), products(0)))
        }
      }
      // Write it to the appropriate column in the product table.
      .write(FijiOutput.builder
          .withTableURI(outputTableUri)
          .withColumnSpecs(Map('support -> ColumnFamilyOutputSpec.builder
              .withFamily(outputColumn)
              .withQualifierSelector('recommendation)
              .build))
          .build)
}
