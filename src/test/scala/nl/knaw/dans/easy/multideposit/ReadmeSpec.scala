/**
 * Copyright (C) 2017 DANS - Data Archiving and Networked Services (info@dans.knaw.nl)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package nl.knaw.dans.easy.multideposit

import java.io.{ByteArrayOutputStream, File}

import nl.knaw.dans.easy.multideposit.CustomMatchers._
import org.apache.commons.configuration.PropertiesConfiguration
import org.scalatest._

class ReadmeSpec extends FlatSpec with Matchers {
  private val helpInfo = {
    val RES_DIR_STR = "src/test/resources/"
    val mockedProps = {
      val ps = new PropertiesConfiguration()
      ps.setDelimiterParsingDisabled(true)
      ps.load(new File(RES_DIR_STR + "debug-config", "application.properties"))
      ps
    }
    val mockedArgs = Array(RES_DIR_STR + "allfields/input", RES_DIR_STR + "allfields/output", "datamanager")
    val mockedStdOut = new ByteArrayOutputStream()
    Console.withOut(mockedStdOut) {
      new ScallopCommandLine(mockedProps, mockedArgs).printHelp()
    }
    mockedStdOut.toString
  }

  "arguments" should "be part of README.md" in {
    val options = helpInfo.replaceAll("\\(default[^)]+\\)","").split("Options:")(1)
    new File("README.md") should containTrimmed(options)
  }

  "synopsis in help info" should "be part of README.md" in {
    val synopsis = helpInfo.split("Options:")(0).split("Usage:")(1)
    new File("README.md") should containTrimmed(synopsis)
  }

  "first banner line" should "be part of README.md and pom.xml" in {
    val description = helpInfo.split("\n")(2)
    new File("README.md") should containTrimmed(description)
    new File("pom.xml") should containTrimmed(s"<description>$description</description>")
  }
}