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

import java.lang.IllegalStateException
import java.util.Arrays

import scala.collection.JavaConverters.asScalaBufferConverter
import scala.collection.JavaConverters.seqAsJavaListConverter

import com.google.common.primitives.UnsignedBytes

import com.moz.fiji.annotations.ApiAudience
import com.moz.fiji.annotations.ApiStability
import com.moz.fiji.annotations.Inheritance
import com.moz.fiji.schema.EntityIdFactory
import com.moz.fiji.schema.{EntityId => JEntityId}

/**
 * An entity ID, or row key, that can be used to address a row in a Fiji table. This is the
 * Express representation of a [[com.moz.fiji.schema.EntityId]].
 *
 * When writing to a Fiji table using FijiExpress, the 'entityId field of each tuple must contain a
 * single [[com.moz.fiji.express.flow.EntityId]].  The rest of the tuple fields will be written to the
 * table according to the field -> column mapping specified by the user in
 * [[com.moz.fiji.express.flow.FijiOutput]], for the same row that the 'entityId indicated.
 *
 * Users can create EntityIds by passing in the objects that compose it. For example, if a Fiji
 * table uses formatted row keys composed of a string as their first component and a long as the
 * second, the user can create this as:
 * {{{
 * EntityId("myString", 1L)
 * }}}
 *
 * When reading from a Fiji table using [[com.moz.fiji.express.flow.FijiInput]], each row is read into a
 * tuple, and the 'entityId field of each tuple is automatically populated with an instance of
 * [[com.moz.fiji.express.flow.EntityId]] corresponding to the EntityID of that row.
 *
 * Users can retrieve the index'th element of an EntityId (0-based), as follows:
 * {{{
 * myEntityId(index)
 * }}}
 *
 * EntityIds can either be [[com.moz.fiji.express.flow.EntityId.MaterializedEntityId]] (in the case of
 * EntityIds from tables with formatted row keys, and all user-created EntityIds), or
 * [[com.moz.fiji.express.flow.EntityId.HashedEntityId]] (in the case of EntityIds from tables with
 * hashed or materialization-suppressed row keys).
 *
 * Note for joining on EntityIds: MaterializedEntityIds can be compared with each other.
 * HashedEntityIds can be compared with other HashedEntityIds, but only their hash values are
 * compared.  You should only attempt to join on HashedEntityIds if they are from the same table.
 * If you join two pipes on EntityIds, and one of them comes straight from a table and contains
 * HashedEntityIds, while the other only has the components of the EntityIds, you can't compare an
 * EntityId constructed directly from the components with the HashedEntityId.  Instead, you need to
 * construct an EntityId containing the hash of the components according to the table your
 * HashedEntityId is from.  You can do this using [[com.moz.fiji.schema.FijiTable#getEntityId]].
 */
@ApiAudience.Public
@ApiStability.Stable
@Inheritance.Sealed
trait EntityId extends Product with Ordered[EntityId] with Serializable {
  /**
   * Get the Java [[com.moz.fiji.schema.EntityId]] associated with this Scala EntityId.
   *
   * @param eidFactory is the EntityIdFactory used to convert the underlying components to a Java
   *     EntityId.
   * @return the Java EntityId backing this EntityId.
   */
  def toJavaEntityId(eidFactory: EntityIdFactory): JEntityId

  /**
   * Get the index'th component of the EntityId.
   *
   * @param index of the component to retrieve.
   * @return the component at index.
   */
  def apply(index: Int): Any = components(index)

  override def productPrefix: String = "EntityId"

  override def canEqual(that: Any): Boolean = that.isInstanceOf[EntityId]

  /**
   * Returns whether this is equal to `other`.  EntityIds are only comparable to other EntityIds.
   *
   * HashedEntityIds can be compared with other HashedEntityIds, but only their hash values are
   * compared.  You should only attempt to join on HashedEntityIds if they are from the same table.
   * If you join two pipes on EntityIds, and one of them comes straight from a table and contains
   * HashedEntityIds, while the other only has the components of the EntityIds, you can't compare an
   * EntityId constructed directly from the components with the HashedEntityId.  Instead, you need
   * to construct an EntityId containing the hash of the components according to the table your
   * HashedEntityId is from.  You can do this using [[com.moz.fiji.schema.FijiTable#getEntityId]].
   *
   * @param obj to compare this entity id to.
   * @return whether the two objects are "equal" according to the definition in this scaladoc.
   */
  override def equals(obj: Any): Boolean = obj match {
    case other: EntityId =>
      try {
        compare(other) == 0
      }
      catch {
        case _: EntityIdFormatMismatchException => false
      }
    case _ => false
  }

  /**
   * Returns the hashcode of the underlying entityId. For a materialized entity id it returns
   * the hashcode of the underlying list of components. For a hashed entityId it, it returns
   * the hashcode of the encoded byte array.
   *
   * @return entityId hashcode.
   */
  override def hashCode(): Int

  /**
   * Returns the underlying components of this entityId. For a materialized entityId, this will
   * be the list of components. For a hashed entityId, this will be a singleton list containing
   * the encoded byte[].
   *
   * @return a list of the underlying components.
   */
  def components: Seq[AnyRef]

  /**
   * Returns the comparison result ( > 0, 0, < 0).
   *
   * MaterializedEntityIds can be compared with each other.
   * HashedEntityIds can be compared with other HashedEntityIds, but only their hash values are
   * compared.  You should only attempt to join on HashedEntityIds if they are from the same table.
   * If you join two pipes on EntityIds, and one of them comes straight from a table and contains
   * HashedEntityIds, while the other only has the components of the EntityIds, you can't compare an
   * EntityId constructed directly from the components with the HashedEntityId.  Instead, you need
   * to construct an EntityId containing the hash of the components according to the table your
   * HashedEntityId is from.  You can do this using [[com.moz.fiji.schema.FijiTable#getEntityId]].
   *
   * @return the comparison result ( > 0, 0, < 0).
   */
  override def compare(rhs: EntityId): Int = {
    val zipped = this.components.zip(rhs.components)
    // Compare each element lexicographically.
    zipped.foreach {
      case (left: Array[Byte], right: Array[Byte]) => {
        val comparison = UnsignedBytes.lexicographicalComparator().compare(left, right)
        if (comparison != 0) return comparison
      }
      case (left: Comparable[AnyRef], right) =>
        try {
          val comparison = left.compareTo(right)
          if (comparison != 0) return comparison
        } catch {
          case _: ClassCastException =>
            throw new EntityIdFormatMismatchException(components, rhs.components)
        }
      case _ => throw new EntityIdFormatMismatchException(components, rhs.components)
    }
    // If all elements in "zipped" were equal, we compare the lengths.
    components.length.compare(rhs.components.length)
  }
}

/**
 * Companion object for EntityId. Provides factory methods and implementations for EntityIds.
 */
@ApiAudience.Public
@ApiStability.Stable
object EntityId {
  /**
   * Creates a FijiExpress EntityId from a Java EntityId.  This is used internally to convert
   * between fiji-schema and fiji-express.
   *
   * Users should not need to use this method.
   *
   * @param entityId is the Java EntityId to convert.
   */
  @ApiAudience.Framework
  def fromJavaEntityId(entityId: JEntityId): EntityId = {
    val hbaseKey = entityId.getHBaseRowKey

    try {
      val components = entityId
          .getComponents
          .asScala
          // Converts the unmodifiable list that FormattedEntityIds use to a scala immutable list
          // which kryo has a configured serializer for.
          .toList
      MaterializedEntityId(components)
    } catch {
      // This is an exception thrown when we try to access components of an entityId which has
      // materialization suppressed. E.g. Hashed EntityIds. So we are unable to retrieve components,
      // but the behavior is legal.
      case ise: IllegalStateException => {
        HashedEntityId(hbaseKey)
      }
    }
  }

  /**
   * Creates a new EntityId with the components specified by the user.
   *
   * @param components of the EntityId to create.
   *
   * @return the created entity id.
   */
  def apply(components: Any*): EntityId = {
    MaterializedEntityId(components.toSeq.map { _.asInstanceOf[AnyRef] })
  }

  /**
   * Creates a new EntityId given an array of bytes representing the raw
   * HBase rowkey.
   *
   * @param encoded is the raw hbase rowkey.
   *
   * @return the created entity id.
   */
  def apply(encoded: Array[Byte]): EntityId = {
    HashedEntityId(encoded)
  }

  /**
   * An EntityId that does not provide access to its components.  It only contains the encoded hash.
   *
   * These are never user-created.  They are constructed by FijiExpress when reading from a table
   * with row key format HASHED or with suppress-materialization enabled.
   *
   * @param encoded byte array representation of this EntityId.
   */
  @ApiAudience.Private
  @ApiStability.Stable
  final private[express] case class HashedEntityId(encoded: Array[Byte])
      extends EntityId {

    override def components: Seq[AnyRef] = List(encoded)

    override def hashCode(): Int = Arrays.hashCode(encoded)

    override def toJavaEntityId(eidFactory: EntityIdFactory): JEntityId = {
      eidFactory.getEntityIdFromHBaseRowKey(encoded)
    }
  }

  /**
   * An EntityId that provides access to its components.  It can be constructed by the user:
   * {{{
   * EntityId(component1, component2, component3)
   * }}}
   *
   * FijiExpress will return an instance of this class in the 'entityId field of the tuple if the
   * row key format in the layout of the table supports returning the components of the EntityId.
   *
   * @param components of an EntityId.
   */
  @ApiAudience.Private
  @ApiStability.Stable
  final private[express] case class MaterializedEntityId(
      override val components: Seq[AnyRef]
  ) extends EntityId {

    override def toJavaEntityId(eidFactory: EntityIdFactory): JEntityId = {
      eidFactory.getEntityId(components.asJava)
    }
  }
}
