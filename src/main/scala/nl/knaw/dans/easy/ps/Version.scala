package nl.knaw.dans.easy.ps

import java.util.Properties

object Version {
  def apply(): String = {
    val props = new Properties()
    props.load(Version.getClass.getResourceAsStream("/Version.properties"))
    props.getProperty("process-sip.version")
  }
}