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

import scala.collection.JavaConverters.seqAsJavaListConverter

import com.moz.fiji.annotations.ApiAudience
import com.moz.fiji.annotations.ApiStability
import com.moz.fiji.annotations.Inheritance
import com.moz.fiji.mapreduce.kvstore.KeyValueStoreReader
import com.moz.fiji.mapreduce.kvstore.lib.{FijiTableKeyValueStore => JFijiTableKeyValueStore}
import com.moz.fiji.schema.FijiColumnName
import com.moz.fiji.schema.FijiRowKeyComponents
import com.moz.fiji.schema.FijiURI

/**
 * A map from keys to values backed by a data store. FijiExpress end users can configure and use
 * key-value stores in FijiExpress. Key-value stores can provide "side data" to steps of a modeling
 * workflow (such as a model computed in another system), and/or be used to pass data generated in
 * one modeling workflow step to another.
 *
 * Developers should see the `ForwardingKeyValueStore` for implementing this trait over an existing
 * FijiMR key-value store.
 *
 * Users should see the `ExpressKeyValueStore` and `ExpressFijiTableKeyValueStore` companion objects
 * for creating instances of this trait based on existing FijiMR key-value store implementations.
 *
 * The companion objects contain factory methods that allow the user to specify functions that
 * operate on keys going into the key-value store and on values coming from the key-value store.
 * Typical use cases of these methods include the following:
 * <ul>
 *  <li>Converting `String` values from a text-based key-value store into integers
 *  <li>Converting `Long` keys in Scalding jobs into `EntityId`s when using a `FijiTable` key-value
 *      store.
 *  <li>Converting values coming out of a `FijiTable` key-value store from types used by Avro for
 *      serialization into primitives (an example below, for example, converts
 *      `org.apache.avro.util.Utf8` values into `String`s)
 * </ul>
 *
 * Users should use `ExpressKeyValueStore`s as shown in the examples below.
 * <ol>
 *  <li>Define a function that uses one of the companion object factory methods described below to
 *      create a reference to an `ExpressKeyValueStore`.
 *  <li>Use the Scalding `com.twitter.scalding.RichPipe.using` method to open the key-value store
 *  <li>Invoke one of the `RichPipe` methods (`foreach`, `map`, `flatMap`) that Scalding make
 *      available within a `using` block.  In addition to having `Fields` arguments, these methods
 *      also take an additional context argument, which in this case is the `ExpressKeyValueStore`.
 *      Users wishing to use a key-value store in multiple `RichPipe` operations (e.g., a `map`
 *      followed by a `foreach`) must invoke `using` twice.
 *  <li>After the pipe operation (`foreach`, `map`, or `flatMap`) completes, Scalding will
 *      automatically close the key-value store.
 * </ol>
 *
 * Below is an example code snippet that uses an `ExpressKeyValueStore` to access data in a
 * `FijiTable`.
 * {{{
 *  // Create an ExpressFijiTableKeyValueStore for use in a Scalding "using" block
 *  def createKeyValueStoreContext: ExpressKeyValueStore[EntityId, String] = {
 *    ExpressFijiTableKeyValueStore[String, Utf8](
 *        tableUri = args("city"),
 *        column = "family:city",
 *        // Avro serializes strings as Utf8, so we use a "valueConverter" function here to convert
 *        // the values to Strings.
 *        valueConverter = (value: Utf8) => value.toString )
 *  }
 *  ...
 *  // Within an Express pipe
 *  // Use the key value store to also get the user's city!
 *  .using(createCityKeyValueStoreContext)
 *    // KVS available for this map command
 *    .map('entityId -> 'city) { (kvs: ExpressKeyValueStore[EntityId, String], eid: EntityId) =>
 *        kvs.getOrElse(eid, "No city!!!") }
 *    //...KVS no longer available, Scalding will automatically call the "close" method
 * }}}
 *
 */
@ApiAudience.Public
@ApiStability.Stable
@Inheritance.Sealed
trait ExpressKeyValueStore[K,V] {
  /**
  * Retrieves the value associated with a key from this key-value store.
  *
  * The key specified must be non-`null`. This method will return `null` if there is no value
  * associated with the key specified. Use the method `#containsKey` to determine whether a key
  * is associated with a value in a key-value store.
  *
  * @param key whose value should be retrieved from the key-value store. Must be non-`null`.
  * @return the value associated with the specified key, or `null` if no such value exists.
  */
  def apply(key: K): V = {
    get(key).getOrElse(
        throw new NoSuchElementException(("No value exists in this KeyValueStore for key '%s' " +
            "Use either the method get() on this KeyValueStore to retrieve optional values, or " +
            "first check that the key is defined using the method containsKey() before using " +
            "this method to lookup the key.").format(key.toString)))
  }

  /**
  * Retrieves an optional value associated with a key from this key-value store.
  *
  * The key specified must be non-`null`. This method will return `None` if there is no value
  * associated with the key specified. Use the method `#containsKey` to determine whether a key
  * is associated with a value in a key-value store.
  *
  * @param key whose value should be retrieved from the key-value store. Must be non-`null`.
  * @return an optional value associated with the specified key.
  */
  def get(key: K): Option[V]

  /**
  * Returns the value associated with a key, or a default value if the key is not contained in
  * the map.
  *
  * @param key whose value should be retrieved, if possible.
  * @param default value to return if key is not contained in the key value store.
  */
  def getOrElse(key: K, default: V): V =  get(key).getOrElse(default)

  /**
   * Determines if the specified key is associated with a value by the key-value store.
   *
   * @param key that may be associated with a value by the key-value store. Must be non-`null`.
   * @return `true` if the specified key is associated with a value by the key-value store,
   *     `false` otherwise.
   */
  def containsKey(key: K): Boolean

  /**
   * Closes all resources used by this key-value store.
   */
  private[fiji] def release(): Unit
}

/**
 * A factory for key-value stores backed by specific FijiMR key-value store implementations.
 */
@ApiAudience.Public
@ApiStability.Stable
object ExpressKeyValueStore {

  /**
   * Creates a new FijiExpress key-value store backed by a FijiMR key-value store reader.  This
   * KeyValueStore does no conversion between keys and values.
   *
   * @param kvStoreReader of underlying FijiMR key-value store.
   * @tparam K type of keys
   * @tparam V type of values
   * @return ExpressKeyValueStore backed by a FijiMR key-value store.
   */
  def apply[K, V](
      kvStoreReader: KeyValueStoreReader[K, V]
  ): ExpressKeyValueStore[K, V] = {
    new ForwardingKeyValueStore[K, V, K, V](kvStoreReader, identity[K], identity[V])
  }

  /**
   * Creates a new FijiExpress key-value store backed by a FijiMR key-value store reader. Conversion
   * functions can be specified for keys and values.
   *
   * The user may wish to use key and value converters when reading data out of a delimited text
   * file if the data needs to be converted from `String` to some other format (e.g., `Double`). It
   * is also useful for casting data coming out of a `FijiTable`, for example, converting `Utf8`
   * values to `String` values.
   *
   * @param kvStoreReader of underlying FijiMR key-value store reader.
   * @param keyConverter A function that converters keys supplied by the user to keys suitable for
   *     the `kvStoreReader`.
   * @param valueConverter A function that converters values read from the `kvStoreReader` into the
   *     format desired by the user.
   * @tparam K type of keys
   * @tparam V type of values
   * @tparam UK is the underlying FijiMR kv-store's key type.
   * @tparam UV is the underlying FijiMR kv-store's value type.
   * @return ExpressKeyValueStore backed by a FijiMR key-value store.
   */
  def apply[K, V, UK, UV](
      kvStoreReader: KeyValueStoreReader[UK, UV],
      keyConverter: K => UK,
      valueConverter: UV => V
  ): ExpressKeyValueStore[K, V] = {
    new ForwardingKeyValueStore[K, V, UK, UV](kvStoreReader, keyConverter, valueConverter)
  }

  /**
   * Converts an [[com.moz.fiji.express.flow.EntityId]] to a java KV-store compatible
   * [[com.moz.fiji.schema.FijiRowKeyComponents]].
   *
   * @param eid to convert.
   * @return the equivalent `FijiRowKeyComponents`.
   */
  def eidConverter(eid: EntityId): FijiRowKeyComponents = {
    // FijiRowKeyComponents can use Java byte array, Java String, Java Long,
    // or Java Integer as components. As long as we ensure all components are AnyRef,
    // then Scala Array[Byte], Scala String, Scala Long, and Scala Int are usable to create a
    // FijiRowKeyComponents. We do this conversion then create the FijiRowKeyComponents.
    FijiRowKeyComponents.fromComponentsList(eid.components.asJava)
  }
}

/**
 * Special factory methods for FijiTable key-value stores.
 */
@ApiAudience.Public
@ApiStability.Stable
object ExpressFijiTableKeyValueStore {

  /**
  * Creates a new FijiExpess key-value store backed by a FijiMR FijiTable key-value store reader.
  * This Express key-value store automatically converts user-supplied entity Ids to
  * `FijiRowKeyComponents` needed to index the underlying `KeyValueStoreReader`.
  *
  * @param kvStoreReader of underlying FijiMR FijiTable key-value store reader.
  * @param valueConverter A function that converters values read from the `kvStoreReader` into the
  *     format desired by the user.
  * @tparam V is the type of value users will retrieve when accessing the key-value store.
  * @tparam UV is the underlying FijiMR kv-store's value type.
  * @return ExpressKeyValueStore backed by a FijiMR key-value store.
  */
  def apply[V, UV](
      kvStoreReader: KeyValueStoreReader[FijiRowKeyComponents, UV],
      valueConverter: UV => V
  ): ExpressKeyValueStore[EntityId, V] = {
    new ForwardingKeyValueStore[EntityId, V, FijiRowKeyComponents, UV](
        kvStoreReader, ExpressKeyValueStore.eidConverter, valueConverter)
  }

  /**
   * Creates a new FijiExpess key-value store backed by a FijiMR FijiTable key-value store reader.
   * This Express key-value store automatically converts user-supplied entity Ids to
   * `FijiRowKeyComponents` needed to index the underlying `KeyValueStoreReader`.
   *
   * @param kvStoreReader of underlying FijiMR FijiTable key-value store reader.
   * @tparam V is the type of value users will retrieve when accessing the key-value store.
   * @return ExpressKeyValueStore backed by a FijiMR key-value store.
   */
  def apply[V](
      kvStoreReader: KeyValueStoreReader[FijiRowKeyComponents, V]
  ): ExpressKeyValueStore[EntityId, V] = {
    new ForwardingKeyValueStore[EntityId, V, FijiRowKeyComponents, V](
      kvStoreReader, ExpressKeyValueStore.eidConverter, identity[V])
  }

  /**
  * Creates a new FijiExpess key-value store backed by a FijiMR FijiTable key-value store reader.
  * This Express key-value store automatically converts user-supplied entity Ids to
  * `FijiRowKeyComponents` needed to index the underlying `KeyValueStoreReader`.
  *
  * This method creates the underlying KeyValueStoreReader for the user, simplifying the client-side
  * code if the user needs to specify only a URI and column name.
  *
  * @param tableUri addressing a table in a Fiji instance.
  * @param column A fully-qualified column name in the specified Fiji instance.
  * @param valueConverter A function that converters values read from the `kvStoreReader` into the
  *     format desired by the user.
  * @tparam V is the type of value users will retrieve when accessing the key-value store.
  * @tparam UV is the underlying FijiMR kv-store's value type.
  * @return ExpressKeyValueStore backed by a FijiMR key-value store.
  */
  def apply[V, UV](
      tableUri: String,
      column: String,
      valueConverter: UV => V
  ): ExpressKeyValueStore[EntityId, V] = {
    val kvStoreReader: KeyValueStoreReader[FijiRowKeyComponents, UV] = JFijiTableKeyValueStore
        .builder()
        .withTable(FijiURI.newBuilder(tableUri).build())
        .withColumn(new FijiColumnName(column))
        .build()
        .open()

    new ForwardingKeyValueStore[EntityId, V, FijiRowKeyComponents, UV](
        kvStoreReader, ExpressKeyValueStore.eidConverter, valueConverter)
  }

  /**
  * Creates a new FijiExpess key-value store backed by a FijiMR FijiTable key-value store reader.
  * This Express key-value store automatically converts user-supplied entity Ids to
  * `FijiRowKeyComponents` needed to index the underlying `KeyValueStoreReader`.
  *
  * This method creates the underlying KeyValueStoreReader for the user, simplifying the client-side
  * code if the user needs to specify only a URI and column name.
  *
  * @param tableUri addressing a table in a Fiji instance.
  * @param column A fully-qualified column name in the specified Fiji instance.
  * @tparam V is the type of value users will retrieve when accessing the key-value store.
  * @return ExpressKeyValueStore backed by a FijiMR key-value store.
  */
  def apply[V](
      tableUri: String,
      column: String
  ): ExpressKeyValueStore[EntityId, V] = {
    apply[V, V](tableUri, column, identity[V] _)
  }
}

/**
 * Implementation-wise, a FijiExpress key-value store is Scala-friendly face on the Java key-value
 * stores provided by the FijiMR library.  This class composes together a FijiMR key-value store and
 * functions that convert keys and values (by default, the identity function).  These key and
 * value conversion function allow you to return a KeyValueStore that provides keys and values of
 * Scala-convenient types, and have them automatically converted to the underlying key-value
 * store's key and value types.
 *
 * @tparam K is the type of key users will specify when accessing the key-value store.
 * @tparam V is the type of value users will retrieve when accessing the key-value store.
 * @tparam UK is the underlying FijiMR kv-store's key type.
 * @tparam UV is the underlying FijiMR kv-store's value type.
 * @param kvStoreReader a FijiMR key-value store that will back this FijiExpress key-value store.
 * @param keyConverter A function that converters keys supplied by the user to keys suitable for the
 *     `kvStoreReader`.
 * @param valueConverter A function that converters values read from the `kvStoreReader` into the
 *     format desired by the user.
 */
@ApiAudience.Public
@ApiStability.Stable
@Inheritance.Sealed
private[fiji] class ForwardingKeyValueStore[K, V, UK, UV](
    kvStoreReader: KeyValueStoreReader[UK, UV],
    keyConverter: K => UK,
    valueConverter: UV => V
) extends ExpressKeyValueStore[K, V] {
  require(kvStoreReader != null)

  override private[fiji] def release(): Unit = kvStoreReader.close()

  override def get(key: K): Option[V] = {
    require(key != null, "A null key was used to access a value from a KeyValueStore.")
    Option(kvStoreReader.get(keyConverter(key))).map(valueConverter)
  }

  override def containsKey(key: K): Boolean =  kvStoreReader.containsKey(keyConverter(key))
}
