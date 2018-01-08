package nl.knaw.dans.easy.multideposit2

import java.nio.file.{ Files, Paths }

import scala.util.Failure
import nl.knaw.dans.easy.multideposit.FileExtensions

object Test extends App {

  val smd = Paths.get("src/test/resources/allfields/input")
  val sd = Paths.get("target/stagingDir")
  val od = Paths.get("target/outputDir")

  sd.deleteDirectory()
  od.deleteDirectory()

  Files.createDirectories(sd)
  Files.createDirectories(od)

  val app = SplitMultiDepositApp(smd, sd, od)
  app.convert()
    .recoverWith { case e => e.printStackTrace(); Failure(e) }
    .foreach(_ => println("success"))
}
