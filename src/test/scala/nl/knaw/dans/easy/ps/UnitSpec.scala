package nl.knaw.dans.easy.ps

import java.io.{IOException, File}
import java.util

import com.typesafe.config.{ConfigFactory, ConfigValue}
import nl.knaw.dans.easy.ps
import org.apache.commons.io.FileUtils._
import org.scalatest._

import scala.collection.JavaConversions._
import scala.collection.immutable.IndexedSeq
import scala.collection.mutable
import scala.util.{Success, Try}

abstract class UnitSpec extends FlatSpec with Matchers with
OptionValues with Inside with Inspectors with OneInstancePerTest {

  /** Location of the cfg and res folders. The cfg/application.conf
    * is persistent over all tests, so this folder is persistent too. */
  val appHome = file("target/test/appHome")

  /** Used if not specified with a command line argument.
    * It is part of the cfg/application.conf
    * which is persistent over all tests, so this folder is persistent too. */
  val defaultSpringfieldInbox = file("target/test/defaultSpringFieldInbox")

  /** loads StorageServices with a mocked but persistent application.conf */
  val mockStorageServices: Map[MdKey, ConfigValue] = {
    defaultSpringfieldInbox.mkdirs()
    val cfgFile = file(appHome, "cfg/application.conf")
    scala.util.Properties.setProp("config.file", cfgFile.toString)
    write(cfgFile, s"springfield-inbox = ${defaultSpringfieldInbox.toString}\n" +
      "storage-services { zandbak = \"zandbak\"}\n" +
      "springfield-streaming-baseurl = \"http://tstreaming11.dans.knaw.nl/site/index.html?presentation=\"\n"
    )
    ConfigFactory.load().getConfig("storage-services").root.toMap
  }

  /**
   * Location of files produced by a test.
   *
   * The complete directory is cleaned up before a test is executed.
   * The results of the last executed test remain available for debugging purposes.
   * Note that an IDE might need some time to refresh if it does refresh automatically at all.
   **/
  val testDir = file(s"target/test/${getClass.getSimpleName}")
  val storageLocation = file(testDir,"storage")

  deleteQuietly(appHome)
  deleteQuietly(defaultSpringfieldInbox)
  deleteQuietly(testDir)

  def dataset(map: Map[MdKey, MdValues]): Dataset = mutable.HashMap[MdKey, MdValues]() ++ map

  def mockStorageConnector: StorageConnector = new StorageConnector() {
    override def exists(url: String): Try[Boolean] = Try(patch(url).exists())

    override def delete(url: String): Try[Unit] = Try(if (!patch(url).delete()) throw new IOException())

    override def createDirectory(url: String): Try[Unit] = Try(if (!patch(url).mkdirs()) throw new IOException())

    override def writeFileTo(sourceFile: File, url: String): Try[Unit] = Try(writeByteArrayToFile(patch(url), readFileToByteArray(sourceFile)))

    /** Replaces the configured storage locations with a folder in the testDir */
    private def patch(url: String): File = {
      // in case the instructions file specifies an URL, the slash in a directory name makes asserts impossible
      file(storageLocation, url.replaceAll("http:/+", ""))
    }
  }

  /** works around CommandLineOptions.parse */
  def mockSettings(storageConnectorInstance: StorageConnector = mockStorageConnector,
                   storageServicesInstance: Map[MdKey, ConfigValue] = mockStorageServices
                    ): ps.Settings = {
    Settings(
      appHomeDir = appHome,
      sipDir = file(testDir, "sip"),
      ebiuDir = file(testDir, "ebiu"),
      springfieldInbox = file(testDir, "springfieldInbox"),
      springfieldStreamingBaseUrl = null, // FIXME
      storageServices = storageServicesInstance,
      storage = storageConnectorInstance
    )
  }
}
