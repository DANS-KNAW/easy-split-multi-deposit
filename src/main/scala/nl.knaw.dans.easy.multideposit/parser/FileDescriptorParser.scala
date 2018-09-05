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
package nl.knaw.dans.easy.multideposit.parser

import better.files.File
import nl.knaw.dans.easy.multideposit.model.{ DepositId, FileAccess, FileDescriptor }
import nl.knaw.dans.lib.error._

import scala.util.{ Failure, Success, Try }

trait FileDescriptorParser {
  this: ParserUtils =>

  def extractFileDescriptors(rows: DepositRows, rowNum: Int, depositId: DepositId): Try[Map[File, FileDescriptor]] = {
    extractList(rows)(fileDescriptor(depositId))
      .flatMap(_.groupBy { case (file, _, _, _) => file }
        .map((toFileDescriptor(rowNum) _).tupled)
        .collectResults
        .map(_.toMap))
  }

  private def toFileDescriptor(rowNum: => Int)(file: File, dataPerPath: List[(File, Option[String], Option[FileAccess.Value], Option[FileAccess.Value])]): Try[(File, FileDescriptor)] = {
    val titles = dataPerPath.collect { case (_, Some(title), _, _) => title }
    val fars = dataPerPath.collect { case (_, _, Some(far), _) => far }
    val fvs = dataPerPath.collect { case (_, _, _, Some(fv)) => fv }

    if (titles.size > 1) Failure(ParseException(rowNum, s"FILE_TITLE defined multiple values for file '$file': ${ titles.mkString("[", ", ", "]") }"))
    else if (fars.size > 1) Failure(ParseException(rowNum, s"FILE_ACCESSIBILITY defined multiple values for file '$file': ${ fars.mkString("[", ", ", "]") }"))
    else if (fvs.size > 1) Failure(ParseException(rowNum, s"FILE_VISIBILITY defined multiple values for file '$file': ${ fvs.mkString("[", ", ", "]") }"))
    else Success((file, FileDescriptor(titles.headOption, fars.headOption, fvs.headOption)))
  }

  def fileDescriptor(depositId: DepositId)(rowNum: => Int)(row: DepositRow): Option[Try[(File, Option[String], Option[FileAccess.Value], Option[FileAccess.Value])]] = {
    val path = row.find("FILE_PATH").map(findPath(depositId))
    val title = row.find("FILE_TITLE")
    val accessRights = fileAccessRight(rowNum)(row)
    val visibility = fileVisibility(rowNum)(row)

    (path, title, accessRights, visibility) match {
      // Check for invalid values. To keep the number of cases limited, we'll fail fast.
      case (Some(Failure(e)), _, _, _) => Some(Failure(ParseException(rowNum, "FILE_PATH does not represent a valid path", e)))
      case (_, _, Some(Failure(e)), _) => Some(Failure(ParseException(rowNum, e.getMessage, e)))
      case (_, _, _, Some(Failure(e))) => Some(Failure(ParseException(rowNum, e.getMessage, e)))

      // No FILE_* attributes at all is fine, but a missing FILE_PATH is not allowed.
      case (None, None, None, None) => None
      case (None, _, _, _) =>
        Some(Failure(ParseException(rowNum, "FILE_TITLE, FILE_ACCESSIBILITY and FILE_VISIBILITY are only allowed if FILE_PATH is also given")))

      // Check if path exists and is a regular file.
      case (Some(Success(p)), t, optAccessibility, optVisibility) =>
        if (p.notExists) Some(Failure(ParseException(rowNum, s"FILE_PATH refers to a non-existent file: '$p'")))
        else if (p.isDirectory) Some(Failure(ParseException(rowNum, s"FILE_PATH points to a directory: '$p'")))
        else Some(Success((p, t, optAccessibility.map(_.get), optVisibility.map(_.get)))) // _.get is safe here, because we have matched on Some(Failure) for both accessibility and visibility above.
    }
  }

  def fileAccessRight(rowNum: => Int)(row: DepositRow): Option[Try[FileAccess.Value]] = {
    row.find("FILE_ACCESSIBILITY")
      .map(acc => FileAccess.valueOf(acc)
        .map(Success(_))
        .getOrElse(Failure(ParseException(rowNum, s"Value '$acc' is not a valid file accessright"))))
  }

  def fileVisibility(rowNum: => Int)(row: DepositRow): Option[Try[FileAccess.Value]] = {
    row.find("FILE_VISIBILITY")
      .map(acc => FileAccess.valueOf(acc)
        .map(Success(_))
        .getOrElse(Failure(ParseException(rowNum, s"Value '$acc' is not a valid file visibility"))))
  }
}
