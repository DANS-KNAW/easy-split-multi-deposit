/**
 * Copyright (C) 2016 DANS - Data Archiving and Networked Services (info@dans.knaw.nl)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package nl.knaw.dans.easy.multideposit

import java.io.ByteArrayOutputStream
import java.nio.file.Paths

import org.apache.commons.configuration.PropertiesConfiguration
import org.scalatest._

class ReadmeSpec extends FlatSpec with Matchers with CustomMatchers {
  private val resourceDirString: String = Paths.get(getClass.getResource("/").toURI).toAbsolutePath.toString

  private val mockedProps = new PropertiesConfiguration() {
    setDelimiterParsingDisabled(true)
    load(Paths.get(resourceDirString + "/debug-config", "application.properties").toFile)
  }

  val mockedArgs = Array("-s", resourceDirString,
    resourceDirString + "/allfields/input",
    resourceDirString + "/allfields/output",
    "datamanager")

  private val clo = new ScallopCommandLine(mockedProps, mockedArgs) {
    // avoids System.exit() in case of invalid arguments or "--help"
    override def verify(): Unit = {}
  }

  private val helpInfo = {
    val mockedStdOut = new ByteArrayOutputStream()
    Console.withOut(mockedStdOut) {
      clo.printHelp()
    }
    mockedStdOut.toString
  }

  "options in help info" should "be part of README.md" in {
    val lineSeparators = s"(${ System.lineSeparator() })+"
    val options = helpInfo.split(s"${ lineSeparators }Options:$lineSeparators")(1)
    options.trim.length shouldNot be(0)
    Paths.get("README.md") should containTrimmed(options)
  }

  "synopsis in help info" should "be part of README.md" in {
    Paths.get("README.md") should containTrimmed(clo.synopsis)
  }

  "description line(s) in help info" should "be part of README.md and pom.xml" in {
    Paths.get("README.md") should containTrimmed(clo.description)
    Paths.get("pom.xml") should containTrimmed(clo.description)
  }
}
