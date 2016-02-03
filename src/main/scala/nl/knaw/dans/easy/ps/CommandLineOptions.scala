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

import com.typesafe.config._
import org.apache.commons.io.FileUtils
import org.rogach.scallop.ScallopConf
import org.slf4j.LoggerFactory

import scala.collection.JavaConversions._
import scala.language.reflectiveCalls


object CommandLineOptions {
  val log = LoggerFactory.getLogger(CommandLineOptions.getClass)
  val sipInstructionsFileName = "instructions.csv"

  def parse(args: Array[String]): Settings = {
    log.debug("Loading application.conf ...")
    val conf = ConfigFactory.load()
    log.debug("Parsing command line ...")
    val opts =
      new ScallopConf(args) {
        printedName = "process-sip"
        version(s"$printedName ${Version()}")
        banner("""Utility to process a Submission Information Package prior to ingestion into the DANS EASY Archive
                |
                |Usage: process-sip.sh [{--username|-u} <user> {--password|-p} <password>] \
                |                      [{--ebiu-dir|-e} <dir>] [{--springfield-inbox|-s} <dir>] <sip-dir>
                |Options:
                |""".stripMargin)
        val sipDir = trailArg[File](name = "sip-dir", required = true,
          descr = "Directory containing the Submission Information Package to process. "
            + s"This must be a valid path to a directory containing a file named '${CommandLineOptions.sipInstructionsFileName}' in "
            + "RFC4180 format.")
        validateOpt(sipDir) {
          case Some(dir) =>
            if (!dir.isDirectory) Left(s"Not a directory '$dir'")
            else if (!FileUtils.directoryContains(dir, new File(dir, CommandLineOptions.sipInstructionsFileName)))
              Left(s"No SIP Instructions found in SIP, expected: '${new File(dir, CommandLineOptions.sipInstructionsFileName)}'")
            else Right(Unit)
          case _ => Left("Could not parse parameter sip-dir")
        }
        val ebiuDir = opt[File]("ebiu-dir",
          descr = "Directory in which EBIU looks for its Dataset Ingest Directories",
          default = Some(new File(System.getenv("HOME") + "/batch/ingest")))
        validateOpt(ebiuDir) {
          case Some(dir) =>
            if (!dir.isDirectory) Left(s"Not a directory '$dir'")
            else Right(Unit)
          case _ => Left("Could not parse parameter ebiu-dir")
        }
        val springfieldInbox = opt[File]("springfield-inbox",
          descr = "Inbox of the Springfield Streaming Media Platform",
          default = Some(new File(conf.getString("springfield-inbox"))))
        validateOpt(springfieldInbox) {
          case Some(dir) =>
            if (!dir.isDirectory) Left(s"Not a directory '$dir'")
            else Right(Unit)
          case _ => Left("Could not parse parameter springfield-inbox")
        }
        val username = opt[String]("username", descr = "The username for alternative storage")
        val password = opt[String]("password", descr = "The password for alternative storage")
        codependent(username, password)
      }
    val user = opts.username.get.getOrElse(askUsername())
    val password = opts.password.get.getOrElse(askPassword())
    val settings = Settings(
      appHomeDir = new File(Option(System.getProperty("app.home")).
                            getOrElse( scala.util.Properties.propOrNull("process.sip.home"))),
      sipDir = opts.sipDir.apply(),
      ebiuDir = opts.ebiuDir.apply(),
      springfieldInbox = opts.springfieldInbox.apply(),
      springfieldStreamingBaseUrl = conf.getString("springfield-streaming-baseurl"),
      storageServices = conf.getConfig("storage-services").root.toMap,
      storage = StorageConnector(user, password))
    log.debug("Using the following settings: {}", settings)
    settings
  }
}
