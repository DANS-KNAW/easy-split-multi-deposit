package nl.knaw.dans.easy.multiDeposit

import java.io.File

import com.typesafe.config.ConfigFactory
import org.apache.commons.io.FileUtils
import org.rogach.scallop.ScallopConf
import org.slf4j.LoggerFactory

import scala.util.Properties

object CommandLineOptions {

  val log = LoggerFactory.getLogger(getClass)
  val mdInstructionsFileName = "instructions.csv"

  def parse(args: Array[String]): Settings = {
    log.debug("Loading application.conf")
    val conf = ConfigFactory.load
    log.debug("Parsing command line")
    val opts = new ScallopCommandLine(args)

    val dir = opts.multiDepositDir.apply()

    val settings = Settings(
      appHomeDir = new File(Option(System.getProperty("app.home"))
        .getOrElse(Properties.propOrNull("process.sip.home"))),
      mdDir = opts.multiDepositDir()
    )

    log.debug("Using the following settings: {}", settings)

    settings
  }
}

class ScallopCommandLine(args: Array[String]) extends ScallopConf(args) {
  import nl.knaw.dans.easy.multiDeposit.{ CommandLineOptions => cmd }

  printedName = "process-multi-deposit"
  version(s"$printedName ${Version()}")
  banner("""Utility to process a Submission Information Package prior to ingestion into the DANS EASY Archive
           |
           |Usage: process-sip.sh <sip-dir>
           |Options:
           |""".stripMargin)

  val multiDepositDir = trailArg[File](name = "multi-deposit-dir", required = true,
    descr = "Directory containing the Submission Information Package to process. "
      + s"This must be a valid path to a directory containing a file named '${cmd.mdInstructionsFileName}' in "
      + "RFC4180 format.")
  validateOpt(multiDepositDir) {
    case Some(dir) => {
      if (!dir.isDirectory)
        Left(s"Not a directory '$dir'")
      else if (!FileUtils.directoryContains(dir, new File(dir, cmd.mdInstructionsFileName)))
        Left(s"No instructions file found in this directory, expected: ${new File(dir, cmd.mdInstructionsFileName)}")
      else
        Right(Unit)
    }
    case None => Left("Could not parse parameter multi-deposit-dir")
  }

  // TODO extend this command line parser
}
