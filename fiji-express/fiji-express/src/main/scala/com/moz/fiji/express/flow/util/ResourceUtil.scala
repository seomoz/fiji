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

package com.moz.fiji.express.flow.util

import java.io.InputStream

import scala.io.Source

import org.apache.hadoop.conf.Configuration

import com.moz.fiji.annotations.ApiAudience
import com.moz.fiji.annotations.ApiStability
import com.moz.fiji.annotations.Inheritance
import com.moz.fiji.commons.ReferenceCountable
import com.moz.fiji.schema.Fiji
import com.moz.fiji.schema.FijiTable
import com.moz.fiji.schema.FijiTableReader
import com.moz.fiji.schema.FijiTableWriter
import com.moz.fiji.schema.FijiURI
import com.moz.fiji.schema.shell.api.Client

/**
 * The Resources object contains various convenience functions while dealing with
 * resources such as Fiji instances or Fiji tables, particularly around releasing
 * or closing them and handling exceptions.
 */
@ApiAudience.Public
@ApiStability.Stable
object ResourceUtil {
  /**
   * Exception that contains multiple exceptions. Typically used in the case where
   * the user gets an exception within their function and further gets an exception
   * during cleanup in finally.
   *
   * @param msg is the message to include with the exception.
   * @param errors causing this exception.
   */
  @ApiAudience.Public
  @ApiStability.Stable
  @Inheritance.Sealed
  final case class CompoundException(msg: String, errors: Seq[Exception]) extends Exception

  /**
   * Performs an operation with a resource that requires post processing. This method will throw a
   * [[com.moz.fiji.express.flow.util.ResourceUtil.CompoundException]] when exceptions get thrown
   * during the operation and while resources are being closed.
   *
   * @tparam T is the return type of the operation.
   * @tparam R is the type of resource such as a Fiji instance or table.
   * @param resource required by the operation.
   * @param after is a function for any post processing on the resource, such as close or release.
   * @param fn is the operation to perform using the resource, like getting the layout of a table.
   * @return the result of the operation.
   * @throws CompoundException if your function crashes as well as the close operation.
   */
  def doAnd[T, R](resource: => R, after: R => Unit)(fn: R => T): T = {
    var error: Option[Exception] = None

    // Build the resource.
    val res: R = resource
    try {
      // Perform the operation.
      fn(res)
    } catch {
      // Store the exception in case close fails.
      case e: Exception => {
        error = Some(e)
        throw e
      }
    } finally {
      try {
        // Cleanup resources.
        after(res)
      } catch {
        // Throw the exception(s).
        case e: Exception => {
          error match {
            case Some(firstErr) => throw CompoundException("Exception was thrown while cleaning up "
                + "resources after another exception was thrown.", Seq(firstErr, e))
            case None => throw e
          }
        }
      }
    }
  }

  /**
   * Performs an operation with a releaseable resource by first retaining the resource and releasing
   * it upon completion of the operation.
   *
   * @tparam T is the return type of the operation.
   * @tparam R is the type of resource, such as a Fiji table or instance.
   * @param resource is the retainable resource object used by the operation.
   * @param fn is the operation to perform using the releasable resource.
   * @return the result of the operation.
   */
  def retainAnd[T, R <: ReferenceCountable[R]](
      resource: => ReferenceCountable[R])(fn: R => T): T = {
    doAndRelease[T, R](resource.retain())(fn)
  }

  /**
   * Performs an operation with an already retained releaseable resource releasing it upon
   * completion of the operation.
   *
   * @tparam T is the return type of the operation.
   * @tparam R is the type of resource, such as a Fiji table or instance.
   * @param resource is the retainable resource object used by the operation.
   * @param fn is the operation to perform using the resource.
   * @return the result of the operation.
   */
  def doAndRelease[T, R <: ReferenceCountable[R]](resource: => R)(fn: R => T): T = {
    def after(r: R) { r.release() }
    doAnd(resource, after)(fn)
  }

  /**
   * Performs an operation with a closeable resource closing it upon completion of the operation.
   *
   * @tparam T is the return type of the operation.
   * @tparam C is the type of resource, such as a Fiji table or instance.
   * @param resource is the closeable resource used by the operation.
   * @param fn is the operation to perform using the resource.
   * @return the result of the operation.
   */
  def doAndClose[T, C <: { def close(): Unit }](resource: => C)(fn: C => T): T = {
    def after(c: C) { c.close() }
    doAnd(resource, after)(fn)
  }

  /**
   * Performs an operation that uses a Fiji instance.
   *
   * @tparam T is the return type of the operation.
   * @param uri of the Fiji instance to open.
   * @param configuration identifying the cluster running Fiji.
   * @param fn is the operation to perform.
   * @return the result of the operation.
   */
  def withFiji[T](uri: FijiURI, configuration: Configuration)(fn: Fiji => T): T = {
    doAndRelease(Fiji.Factory.open(uri, configuration))(fn)
  }

  /**
   * Performs an operation that uses a Fiji table.
   *
   * @tparam T is the return type of the operation.
   * @param fiji instance the desired table belongs to.
   * @param table name of the Fiji table to open.
   * @param fn is the operation to perform.
   * @return the result of the operation.
   */
  def withFijiTable[T](fiji: Fiji, table: String)(fn: FijiTable => T): T = {
    doAndRelease(fiji.openTable(table))(fn)
  }

  /**
   * Performs an operation that uses a Fiji table.
   *
   * @tparam T is the return type of the operation.
   * @param tableUri addressing the Fiji table to open.
   * @param configuration identifying the cluster running Fiji.
   * @param fn is the operation to perform.
   * @return the result of the operation.
   */
  def withFijiTable[T](tableUri: FijiURI, configuration: Configuration)(fn: FijiTable => T): T = {
    withFiji(tableUri, configuration) { fiji: Fiji =>
      withFijiTable(fiji, tableUri.getTable)(fn)
    }
  }

  /**
   * Performs an operation that uses a Fiji table writer.
   *
   * @tparam T is the return type of the operation.
   * @param table the desired Fiji table writer belongs to.
   * @param fn is the operation to perform.
   * @return the result of the operation.
   */
  def withFijiTableWriter[T](table: FijiTable)(fn: FijiTableWriter => T): T = {
    doAndClose(table.openTableWriter)(fn)
  }

  /**
   * Performs an operation that uses a Fiji table writer.
   *
   * @tparam T is the return type of the operation.
   * @param tableUri addressing the Fiji table to open.
   * @param configuration identifying the cluster running Fiji.
   * @param fn is the operation to perform.
   * @return the result of the operation.
   */
  def withFijiTableWriter[T](
      tableUri: FijiURI,
      configuration: Configuration)(fn: FijiTableWriter => T): T = {
    withFijiTable(tableUri, configuration) { table: FijiTable =>
      withFijiTableWriter(table)(fn)
    }
  }

  /**
   * Performs an operation that uses a Fiji table reader.
   *
   * @tparam T is the return type of the operation.
   * @param table the desired Fiji table reader belongs to.
   * @param fn is the operation to perform.
   * @return the result of the operation.
   */
  def withFijiTableReader[T](table: FijiTable)(fn: FijiTableReader => T): T = {
    doAndClose(table.openTableReader)(fn)
  }

  /**
   * Performs an operation that uses a Fiji table reader.
   *
   * @tparam T is the return type of the operation.
   * @param tableUri addressing the Fiji table to open.
   * @param configuration identifying the cluster running Fiji.
   * @param fn is the operation to perform.
   * @return the result of the operation.
   */
  def withFijiTableReader[T](
      tableUri: FijiURI,
      configuration: Configuration)(fn: FijiTableReader => T): T = {
    withFijiTable(tableUri, configuration) { table: FijiTable =>
      withFijiTableReader(table)(fn)
    }
  }

  /**
   * Reads a resource from the classpath into a string.
   *
   * @param path to the desired resource (of the form: "path/to/your/resource").
   * @return the contents of the resource as a string.
   */
  def resourceAsString(path: String): String = {
    val inputStream = getClass
        .getClassLoader
        .getResourceAsStream(path)

    doAndClose(Source.fromInputStream(inputStream)) { source =>
      source.mkString
    }
  }

  /**
   * Executes a series of FijiSchema Shell DDL commands, separated by `;`.
   *
   * @param fiji to execute the commands against.
   * @param commands to execute against the Fiji instance.
   */
  def executeDDLString(fiji: Fiji, commands: String) {
    doAndClose(Client.newInstance(fiji.getURI)) { ddlClient =>
      ddlClient.executeUpdate(commands)
    }
  }

  /**
   * Executes a series of FijiSchema Shell DDL commands, separated by `;`.
   *
   * @param fiji to execute the commands against.
   * @param stream to read a series of commands to execute against the Fiji instance.
   */
  def executeDDLStream(fiji: Fiji, stream: InputStream) {
    doAndClose(Client.newInstance(fiji.getURI)) { ddlClient =>
      ddlClient.executeStream(stream)
    }
  }

  /**
   * Executes a series of FijiSchema Shell DDL commands, separated by `;`.
   *
   * @param fiji to execute the commands against.
   * @param resourcePath to the classpath resource that a series of commands to execute
   *     against the Fiji instance will be read from.
   */
  def executeDDLResource(fiji: Fiji, resourcePath: String) {
    executeDDLStream(fiji, getClass.getClassLoader.getResourceAsStream(resourcePath))
  }
}
