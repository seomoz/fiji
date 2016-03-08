package com.moz.fiji.schema.shell.util

import java.util.UUID

import org.apache.hadoop.hbase.HBaseConfiguration

import com.moz.fiji.schema.FijiInstaller
import com.moz.fiji.schema.FijiURI
import com.moz.fiji.schema.shell.DDLParser
import com.moz.fiji.schema.shell.FijiSystem
import com.moz.fiji.schema.shell.Environment
import com.moz.fiji.schema.shell.input.NullInputSource

trait FijiTestHelpers {
  /**
   * @return the name of a unique Fiji instance (that doesn't yet exist).
   */
  def getNewInstanceURI(): FijiURI = {
    val instanceName = UUID.randomUUID().toString().replaceAll("-", "_");
    return FijiURI.newBuilder("fiji://.env/" + instanceName).build()
  }

  /**
   * Install a Fiji instance.
   */
  def installFiji(instanceURI: FijiURI): Unit = {
    FijiInstaller.get().install(instanceURI, HBaseConfiguration.create())
  }

  /**
   * Get a new parser that's primed with the specified environment.
   */
  def getParser(environment: Environment): DDLParser = {
    new DDLParser(environment)
  }
}
