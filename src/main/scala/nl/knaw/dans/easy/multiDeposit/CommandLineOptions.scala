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
package nl.knaw.dans.easy.multideposit

import java.io.File

import com.typesafe.config.{Config, ConfigFactory}
import nl.knaw.dans.easy.multideposit.CommandLineOptions._
import org.rogach.scallop.{ScallopConf, singleArgConverter}
import org.slf4j.LoggerFactory

object CommandLineOptions {

  val log = LoggerFactory.getLogger(getClass)

  def parse(args: Array[String]): Settings = {
    log.debug("Loading application.conf ...")
    lazy val conf = ConfigFactory.load
    log.debug("Parsing command line ...")
    val opts = new ScallopCommandLine(conf, args)

    val settings = Settings(
      appHomeDir = Option(System.getProperty("app.home"))
        .map(new File(_))
        .getOrElse(throw new IllegalArgumentException("The property 'app.home' is not available " +
          "in the system properties.")),
      multidepositDir = opts.multiDepositDir(),
      springfieldInbox = opts.springfieldInbox(),
      outputDepositDir = opts.outputDepositDir())

    log.debug(s"Using the following settings: $settings")

    settings
  }
}

class ScallopCommandLine(conf: => Config, args: Array[String]) extends ScallopConf(args) {

  val fileMayNotExist = singleArgConverter(new File(_))
  val fileShouldExist = singleArgConverter(filename => {
    val file = new File(filename)
    if (!file.exists) {
      log.error(s"The directory '$filename' does not exist")
      throw new IllegalArgumentException(s"The directory '$filename' does not exist")
    }
    if (!file.isDirectory) {
      log.error(s"'$filename' is not a directory")
      throw new IllegalArgumentException(s"'$filename' is not a directory")
    }
    else file
  })

  printedName = "easy-split-multi-deposit"
  version(s"$printedName ${Version()}")
  banner(s"""Utility to process a Multi-Deposit prior to ingestion into the DANS EASY Archive
           |
           |Usage: $printedName.sh [{--springfield-inbox|-s} <dir>] <multi-deposit-dir> <output-deposits-dir>
           |Options:
           |""".stripMargin)

  lazy val multiDepositDir = trailArg[File](name = "multi-deposit-dir", required = true,
    descr = "Directory containing the Submission Information Package to process. "
      + "This must be a valid path to a directory containing a file named "
      + s"'$instructionsFileName' in RFC4180 format.")(fileShouldExist)
  validateOpt(multiDepositDir)(_.map(file =>
    if (!file.isDirectory) {
      Left(s"Not a directory '$file'")
    }
    else if (!file.directoryContains(new File(file, instructionsFileName)))
      Left("No instructions file found in this directory, expected: "
        + s"${new File(file, instructionsFileName)}")
    else
      Right(()))
    .getOrElse(Left("Could not parse parameter multi-deposit-dir")))

  lazy val springfieldInbox = opt[File]("springfield-inbox",
    descr = "The inbox directory of a Springfield Streaming Media Platform installation. " +
      "If not specified the value of springfield-inbox in application.properties is used.",
    default = Some(new File(conf.getString("springfield-inbox"))))(fileShouldExist)
  validateOpt(springfieldInbox)(_.map(file =>
    if (!file.isDirectory)
      Left(s"Not a directory '$file'")
    else
      Right(()))
    .getOrElse(Left("Could not parse parameter 'springfield-inbox'")))

  lazy val outputDepositDir = trailArg[File](name = "deposit-dir", required = true,
    descr = "A directory in which the deposit directories must be created. "
      + "The deposit directory layout is described in the easy-deposit documenation")(fileMayNotExist)
}
