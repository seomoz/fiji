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

package com.moz.fiji.schema.shell.modules

import com.moz.fiji.annotations.ApiStability
import com.moz.fiji.annotations.ApiAudience
import com.moz.fiji.schema.security.{FijiUser, FijiPermissions}
import com.moz.fiji.schema.shell.spi.ParserPlugin
import com.moz.fiji.schema.shell.spi.ParserPluginFactory
import com.moz.fiji.schema.shell.spi.HelpPlugin
import com.moz.fiji.schema.shell.DDLException
import com.moz.fiji.schema.shell.DDLParserHelpers
import com.moz.fiji.schema.shell.Environment
import com.moz.fiji.schema.shell.ddl.DDLCommand
import com.moz.fiji.schema.{Fiji, KConstants, FijiURI}

/**
 * A plugin for commands for Fiji access permissions.
 *
 * Adds commands for granting and revoking access to users on Fiji instaces.
 */
@ApiAudience.Private
@ApiStability.Experimental
class SecurityParserPluginFactory  extends ParserPluginFactory with HelpPlugin {
  override def getName(): String = "security"

  override def helpText(): String = {
    return """ Adds commands to interact with permissions in Fiji from the Fiji shell.
             |   Parser plugin that matches the syntax:
             |
             |   GRANT { READ | WRITE | GRANT } [PRIVILEGES]
             |        ON INSTANCE 'fiji://instance_uri'
             |        TO [ USER ] user_name;
             |
             |    REVOKE { READ | WRITE | GRANT } [PRIVILEGES]
             |        ON INSTANCE 'fiji://instance_uri'
             |        FROM [ USER ] 'user_name';
             |""".stripMargin
  }

  override def create(env: Environment): ParserPlugin = {
    return new SecurityParserPlugin(env);
  }

  /**
   * Represents a change in permissions that should be executed by Fiji.
   */
  sealed trait PermissionsChange

  /**
   * Represents a grant command that should be executed by Fiji.
   *
   * @param actions to grant.
   * @param instance to grant them on.
   * @param user to whom to grant the permissions for actions on instance.
   */
  case class GrantCommand(
      actions: List[FijiPermissions.Action],
      instance: FijiURI,
      user: FijiUser)
      extends PermissionsChange

  /**
   * Represents a revoke command that should be executed by Fiji.
   *
   * @param actions to revoke.
   * @param instance to revoke them on.
   * @param user from whom to revoke the permissions for actions on instance.
   */
  case class RevokeCommand(
      actions: List[FijiPermissions.Action],
      instance: FijiURI,
      user: FijiUser)
      extends PermissionsChange

  /**
   * Parser plugin that matches the syntax:
   *
   * GRANT { READ | WRITE | GRANT } [PRIVILEGES]
   *     ON INSTANCE 'fiji://instance_uri'
   *     TO [ USER ] user_name
   *
   * REVOKE { READ | WRITE | GRANT } [PRIVILEGES]
   *     ON INSTANCE 'fiji://instance_uri'
   *     FROM [ USER ] 'user_name'
   */
  final class SecurityParserPlugin(val env: Environment)
      extends ParserPlugin
      with DDLParserHelpers {

    // Specifies what instance access is being changed on.
    def instanceSpec: Parser[FijiURI] = (
      i("ON") ~> i("INSTANCE") ~> singleQuotedString ^^ { case uri =>
        FijiURI.newBuilder(uri).build() }
      )

    // Specifies which user's access is being changed in a GRANT.
    def userGrantSpec: Parser[FijiUser] = (
      i("TO") ~> opt(i("USER")) ~> optionallyQuotedString ^^
          { case username => FijiUser.fromName(username) }
      )

    // Specifies which user's access is being changed in a REVOKE.
    def userRevokeSpec: Parser[FijiUser] = (
      i("FROM") ~> opt(i("USER")) ~> optionallyQuotedString ^^
        { case username => FijiUser.fromName(username) }
      )

    // Full GRANT command.
    def grant: Parser[GrantCommand] = (
      i("GRANT") ~> rep(action) ~ opt(i("PRIVILEGES")) ~ instanceSpec ~ userGrantSpec ^^
        { case actions ~ _ ~ instance ~ user => new GrantCommand(actions, instance, user)}
      )

    // Full REVOKE command.
    def revoke: Parser[RevokeCommand] = (
      i("REVOKE") ~> rep(action) ~ opt(i("PRIVILEGES")) ~ instanceSpec ~ userRevokeSpec ^^
        { case actions ~ _ ~ instance ~ user => new RevokeCommand(actions, instance, user)}
      )

    def action: Parser[FijiPermissions.Action] = (
        i("READ") ^^ (_ => FijiPermissions.Action.READ)
      | i("WRITE") ^^ (_ => FijiPermissions.Action.WRITE)
      | i("GRANT") ^^ (_ => FijiPermissions.Action.GRANT))

    override def command(): Parser[DDLCommand] = ( (grant | revoke)  <~ ";"
        ^^ { case permissionsChange => new UserSecurityCommand(env, permissionsChange) }
      )
  }

  final class UserSecurityCommand(
      val env: Environment,
      val permissionsChange: PermissionsChange)
      extends DDLCommand {

    override def exec(): Environment = {
      permissionsChange match {
        case GrantCommand(actions, instance, user) => {
          actions.map { case action: FijiPermissions.Action =>
            env.fijiSystem.getSecurityManager(instance).grant(user, action)
          }
        }
        case RevokeCommand(actions, instance, user) => {
          actions.map { case action: FijiPermissions.Action =>
            env.fijiSystem.getSecurityManager(instance).revoke(user, action)
          }
        }
      }
      return env
    }
  }
}
