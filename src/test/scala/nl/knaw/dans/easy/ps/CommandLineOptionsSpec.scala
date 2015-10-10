package nl.knaw.dans.easy.ps

import java.io.File

import com.typesafe.config.{ConfigException, ConfigValue}
import org.apache.commons.io.FileUtils._

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
