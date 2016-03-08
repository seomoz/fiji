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

import com.moz.fiji.annotations.ApiAudience
import com.moz.fiji.schema.avro.TableLayoutDesc
import com.moz.fiji.schema.shell.DDLException
import com.moz.fiji.schema.shell.Environment
import com.moz.fiji.schema.shell.EmptySchemaUsageFlags
import com.moz.fiji.schema.shell.DeveloperTableValidationPolicy
import com.moz.fiji.schema.shell.LegacyTableValidationPolicy
import com.moz.fiji.schema.shell.NoTableValidationPolicy
import com.moz.fiji.schema.shell.SchemaUsageFlags
import com.moz.fiji.schema.shell.StrictTableValidationPolicy
import com.moz.fiji.schema.shell.TableValidationPolicy
import com.moz.fiji.schema.util.ProtocolVersion

/**
 * Additional context associated with creating or manipulating schemas within a table's CellSchema
 * descriptors.
 *
 * Objects of type CellSchemaContext are created to access state about a table
 * that influences operations such as adding, removing, or modifying definitions
 * of columns and column families and their schemas.
 *
 * @param env the current FijiShell operating environment.
 * @param tableName the name of the table being modified/created/etc.
 * @param layoutVersion the ProtocolVersion describing the capability support level
 *     of the layout descriptor.
 * @param schemaUsageFlags represents input as to how the schema we are currently operating on
 *     should be applied to the layout of a given column. This may be EmptySchemaUsageFlags if
 *     it is not relevant to the current operation.
 */
@ApiAudience.Private
final class CellSchemaContext(val env: Environment, val tableName: String,
    val layoutVersion: ProtocolVersion, val schemaUsageFlags: SchemaUsageFlags) {

  /** Helper object to access constants in the TableProperties trait. */
  private object CellSchemaContextHelper extends TableProperties;

  private val MinLayoutForValidation: ProtocolVersion = ProtocolVersion.parse("layout-1.3.0")

  /**
   * Return the preferred validation strategy for columns in this table (if it is
   * in a validation-capable table layout / system format).
   *
   * <p>This method should not be called if the table does not support validation.</p>
   *
   * @return the TableValidationPolicy case object specifying the validation mode to use.
   */
  def getValidationPolicy(): TableValidationPolicy = {
    // TODO: Cache this value so we don't hit the metatable so often.
    val validationPref: String = env.fijiSystem.getMeta(
        env.instanceURI, tableName, CellSchemaContextHelper.TableValidationMetaKey)
        .getOrElse(CellSchemaContextHelper.DefaultValidationPolicy)

    validationPref match {
      case "NONE" => { return NoTableValidationPolicy }
      case "LEGACY" => { return LegacyTableValidationPolicy }
      case "STRICT" => { return StrictTableValidationPolicy }
      case "DEVELOPER" => { return DeveloperTableValidationPolicy }
      case _ => { throw new DDLException("Unknown table validation policy: " + validationPref) }
    }
  }

  /** @return true if the layout version indicated supports layout upgrade validation. */
  def supportsLayoutValidation(): Boolean = {
    return layoutVersion.compareTo(MinLayoutForValidation) >= 0
  }
}

object CellSchemaContext {
  /**
   * Create a new CellSchemaContext given an environment and a TableLayoutDesc builder.
   */
  def create(env: Environment, layout: TableLayoutDesc.Builder,
      schemaFlags: SchemaUsageFlags = EmptySchemaUsageFlags): CellSchemaContext = {
    return new CellSchemaContext(env, layout.getName(), ProtocolVersion.parse(layout.getVersion()),
        schemaFlags)
  }
}
