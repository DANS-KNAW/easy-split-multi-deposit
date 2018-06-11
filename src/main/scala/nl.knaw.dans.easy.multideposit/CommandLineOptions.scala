/**
 * Copyright (C) 2016 DANS - Data Archiving and Networked Services (info@dans.knaw.nl)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package nl.knaw.dans.easy.multideposit

import java.nio.file.{ Path, Paths }

import better.files.File
import better.files.File._
import nl.knaw.dans.easy.multideposit.model.Datamanager
import org.rogach.scallop.{ ScallopConf, ScallopOption }

import scala.xml.Properties

class CommandLineOptions(args: Array[String], version: String) extends ScallopConf(args) {

  appendDefaultToDescription = false
  editBuilder(_.setHelpWidth(110))

  printedName = "easy-split-multi-deposit"
  val description = "Splits a Multi-Deposit into several deposit directories for subsequent ingest into the archive"
  val synopsis: String =
    s"  $printedName [{--staging-dir|-s} <dir>] [{--identifier-info | -i} <file>] [{--validate|-v}] <multi-deposit-dir> <output-deposits-dir> <datamanager>"

  version(s"$printedName v$version")
  banner(
    s"""
       |  $description
       |  Utility to process a Multi-Deposit prior to ingestion into the DANS EASY Archive
       |
       |Usage:
       |
       |  $synopsis
       |
       |Options:
       |""".stripMargin)

  val stagingDir: ScallopOption[Path] = opt[Path](
    name = "staging-dir",
    short = 's',
    descr = "A directory in which the deposit directories are created, after which they will be " +
      "moved to the 'output-deposit-dir'. If not specified, the value of 'staging-dir' in " +
      "'application.properties' is used.")

  val reportFile: ScallopOption[Path] = opt[Path](
    name = "identifier-info",
    short = 'i',
    descr = "CSV file in which information about the created deposits is reported",
    default = Some(Paths.get(Properties.userHome).resolve(s"easy-split-multi-deposit-identifier-info-$now.csv"))
  )

  val validateOnly: ScallopOption[Boolean] = opt[Boolean](
    name = "validate",
    short = 'v',
    descr = "Only validates the input of a Multi-Deposit ingest",
    default = Some(false))

  val multiDepositDir: ScallopOption[Path] = trailArg[Path](
    name = "multi-deposit-dir",
    required = true,
    descr = "Directory containing the Submission Information Package to process. "
      + "This must be a valid path to a directory containing a file named "
      + s"'${ PathExplorer.instructionsFileName }' in RFC4180 format.")

  val outputDepositDir: ScallopOption[Path] = trailArg[Path](
    name = "output-deposit-dir",
    required = true,
    descr = "A directory to which the deposit directories are moved after the staging has been " +
      "completed successfully. The deposit directory layout is described in the easy-sword2 " +
      "documentation")

  val datamanager: ScallopOption[Datamanager] = trailArg[Datamanager](
    name = "datamanager",
    required = true,
    descr = "The username (id) of the datamanger (archivist) performing this deposit")

  validatePathExists(multiDepositDir)
  validateFileIsDirectory(multiDepositDir.map(_.toFile))
  validate(multiDepositDir)(dir => {
    val instructionFile: File = PathExplorer.multiDepositInstructionsFile(dir)
    if (!dir.isParentOf(instructionFile))
      Left(s"No instructions file found in this directory, expected: $instructionFile")
    else
      Right(())
  })

  validatePathExists(reportFile.map(_.getParent))
  validateFileIsDirectory(reportFile.map(_.getParent.toFile))

  validatePathExists(outputDepositDir)
  validateFileIsDirectory(outputDepositDir.map(_.toFile))

  verify()
}
