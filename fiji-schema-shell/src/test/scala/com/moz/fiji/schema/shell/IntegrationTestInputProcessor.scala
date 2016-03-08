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

package com.moz.fiji.schema.shell

import org.specs2.mutable.SpecificationWithJUnit

import com.moz.fiji.schema.FijiURI
import com.moz.fiji.schema.shell.input.StringInputSource
import com.moz.fiji.schema.shell.util.FijiTestHelpers

class IntegrationTestInputProcessor
    extends SpecificationWithJUnit
    with FijiTestHelpers {

  val testFijiSystem = ShellMain.shellFijiSystem

  val validExpr = """help;"""

  val invalidExpr = "aaa;"

  val validExprNotOnClasspath =
    """
      |CREATE TABLE foo WITH DESCRIPTION 'some data'
      |ROW KEY FORMAT RAW
      |PROPERTIES ( NUMREGIONS = 10 )
      |WITH LOCALITY GROUP default WITH DESCRIPTION 'main storage' (
      |  MAXVERSIONS = INFINITY,
      |  TTL = FOREVER,
      |  INMEMORY = false,
      |  COMPRESSED WITH GZIP,
      |  FAMILY default WITH DESCRIPTION 'basic information' (
      |    info CLASS nonexistent.record WITH DESCRIPTION 'User info')
      |);
    """.stripMargin

  val validExprOnClasspath =
    """
      |CREATE TABLE foo WITH DESCRIPTION 'some data'
      |ROW KEY FORMAT RAW
      |PROPERTIES ( NUMREGIONS = 1 )
      |WITH LOCALITY GROUP default WITH DESCRIPTION 'main storage' (
      |  MAXVERSIONS = INFINITY,
      |  TTL = FOREVER,
      |  INMEMORY = false,
      |  COMPRESSED WITH GZIP,
      |  FAMILY default WITH DESCRIPTION 'basic information' (
      |    info CLASS com.moz.fiji.schema.shell.avro.XYRecord WITH DESCRIPTION 'User info')
      |);
    """.stripMargin

  def envFromInput(uri: FijiURI, input: String, isInteractive: Boolean): Environment = {
    new Environment(
      instanceURI = uri,
      printer = Console.out,
      fijiSystem = testFijiSystem,
      inputSource = new StringInputSource(input),
      modules = List(),
      isInteractive = isInteractive,
      extensionMapping = Map(),
      libJars = List())
  }

  "InputProcessor" should {
    "Not error on successful execution of a non-interactive command" in {
      val inputProcessor = new InputProcessor(throwOnErr = true)

      val uri1 = getNewInstanceURI()
      val env1 = envFromInput(uri1, validExpr, false)
      (inputProcessor.processUserInput(new StringBuilder, env1) must not throwA)

      val uri2 = getNewInstanceURI()
      installFiji(uri2)
      val env2 = envFromInput(uri2, validExprOnClasspath, false)
      (inputProcessor.processUserInput(new StringBuilder, env2) must not throwA)
    }

    "Throw an error on failure of a non-interactive command" in {
      val inputProcessor = new InputProcessor(throwOnErr = true)

      val uri1 = getNewInstanceURI()
      val env1 = envFromInput(uri1, invalidExpr, false)
      (inputProcessor.processUserInput(new StringBuilder, env1) must throwA[DDLException])

      val uri2 = getNewInstanceURI()
      installFiji(uri2)
      val env2 = envFromInput(uri2, validExprNotOnClasspath, false)
      (inputProcessor.processUserInput(new StringBuilder, env2) must throwA[DDLException])
    }

    "Not error on failure of an interactive command" in {
      val inputProcessor = new InputProcessor(throwOnErr = false)

      val uri = getNewInstanceURI()
      val env = envFromInput(uri, invalidExpr, true)
      (inputProcessor.processUserInput(new StringBuilder, env) must not throwA)
    }
  }

  // Shut down the test fiji system after all tests have run.
  step(testFijiSystem.shutdown())
}
