/**
 * Copyright (C) 2015-2016 DANS - Data Archiving and Networked Services (info@dans.knaw.nl)
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
package nl.knaw.dans.easy.ps

import java.io.File

import com.typesafe.config.{ConfigException, ConfigValue}
import org.apache.commons.io.FileUtils._
import org.scalatest.Ignore

@Ignore
class CommandLineOptionsSpec extends UnitSpec {

  { // prepare environment
    if (System.getenv("PROCESS_SIP_HOME") == null) {
      //System.getenv().put("PROCESS_SIP_HOME","src/main/assembly/dist") // throws UnsupportedOperationException
      scala.util.Properties.setProp("process.sip.home", file(testDir,"appHome").toString)
    }
    file(testDir, "ebiu").mkdirs()
    file(testDir, "springFieldInbox").mkdirs()
  }

  "parse" should "succeed with long version of all arguments" in {

    file(testDir, "someSpringFieldInbox").mkdirs()
    write(file(testDir, "sip/instructions.csv"), "")
    val s = CommandLineOptions.parse({{Array(
          "--ebiu-dir", file(testDir, "ebiu").toString,
          "--springfield-inbox", file(testDir, "someSpringFieldInbox").toString,
          "--username", "userName",
          "--password", "password",
          file(testDir, "sip").toString
    )}})
    s.springfieldInbox should equal(file(testDir, "someSpringFieldInbox"))
  }

  it should "use the default springfield inbox" in {

    file("target/test/defaultSpringFieldInbox").mkdirs()
    write(file(testDir, "sip/instructions.csv"), "")
    val s = CommandLineOptions.parse({{Array(
      "--ebiu-dir", file(testDir, "ebiu").toString,
      "--username", "userName",
      "--password", "password",
      file(testDir, "sip").toString
    )}})
    s.springfieldInbox should equal (defaultSpringfieldInbox)
  }

  ignore should "exit if specified sip directory does not exist" in {

    // an exit terminates even the test framework
    CommandLineOptions.parse(Array(file(testDir, "sip").toString))
  }

  ignore should "exit if sip has no instructions.csv" in {

    val sipDir: File = file(testDir, "sip")
    sipDir.mkdirs()
    CommandLineOptions.parse(Array(sipDir.toString))
  }
}
