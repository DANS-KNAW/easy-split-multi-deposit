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
package nl.knaw.dans.easy.multiDeposit

import java.io.File

import com.typesafe.config.{Config, ConfigFactory}
import org.rogach.scallop.ScallopConf
import org.slf4j.LoggerFactory

import scala.util.Properties

object CommandLineOptions {

  val log = LoggerFactory.getLogger(getClass)
  val mdInstructionsFileName = "instructions.csv"

  def parse(args: Array[String]): Settings = {
    log.debug("Loading application.conf ...")
    val conf = ConfigFactory.load
    log.debug("Parsing command line ...")
    val opts = new ScallopCommandLine(conf, args)

    val settings = Settings(
      appHomeDir = new File(Option(System.getProperty("app.home"))
        .getOrElse(Properties.propOrNull("process.sip.home"))),
      multidepositDir = opts.multiDepositDir(),
      springfieldInbox = opts.springfieldInbox(),
      depositDir = opts.depositDir())

    log.debug("Using the following settings: {}", settings)

    settings
  }
}

class ScallopCommandLine(conf: Config, args: Array[String]) extends ScallopConf(args) {
  import nl.knaw.dans.easy.multiDeposit.{CommandLineOptions => cmd}

  printedName = "process-multi-deposit"
  version(s"$printedName ${Version()}")
  banner("""Utility to process a Submission Information Package prior to ingestion into the DANS EASY Archive
           |
           |Usage: process-sip.sh [{--output-deposits-dir|-d} <dir>][{--springfield-inbox|-s} <dir>] <multi-deposit-dir>
           |Options:
           |""".stripMargin)
  // TODO adjust banner because of change in output-deposits-dir being a trailArg now?

  lazy val multiDepositDir = trailArg[File](name = "multi-deposit-dir", required = true,
    descr = "Directory containing the Submission Information Package to process. "
      + s"This must be a valid path to a directory containing a file named '${cmd.mdInstructionsFileName}' in "
      + "RFC4180 format.")
  validateOpt(multiDepositDir)(_.map(file =>
      if (!file.isDirectory)
        Left(s"Not a directory '$file'")
      else if (!file.directoryContains(new File(file, cmd.mdInstructionsFileName)))
        Left(s"No instructions file found in this directory, expected: ${new File(file, cmd.mdInstructionsFileName)}")
      else
        Right(()))
      .getOrElse(Left("Could not parse parameter multi-deposit-dir")))

  lazy val springfieldInbox = opt[File]("springfield-inbox",
    descr = "The inbox directory of a Springfield Streaming Media Platform installation. " +
      "If not specified the value of springfield-inbox in application.properties is used.",
    default = Some(new File(conf.getString("springfield-inbox"))))
  validateOpt(springfieldInbox)(_.map(file =>
    if (!file.isDirectory)
      Left(s"Not a directory '$file'")
    else
      Right(()))
    .getOrElse(Left("Could not parse parameter 'springfield-inbox'")))

  lazy val depositDir = trailArg[File](name = "deposit-dir", required = true,
    descr = "A directory in which the deposit directories must be created. " +
      "The deposit directory layout is described in the easy-deposit documenation")
  validateOpt(depositDir)(_.map(file =>
    if (!file.isDirectory)
      Left(s"Not a directory '$file'")
    else
      Right(()))
    .getOrElse(Left("Could not parse parameter 'deposit-dir'")))
}
