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
import nl.knaw.dans.easy.multideposit.model.{ DepositId, FileAccessRights, FileDescriptor }
import nl.knaw.dans.lib.error._

import scala.collection.mutable.ListBuffer
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

  private def toFileDescriptor(rowNum: => Int)(file: File, dataPerPath: List[(File, Option[String], Option[FileAccessRights.Value], Option[FileAccessRights.Value])]): Try[(File, FileDescriptor)] = {
    val titles = dataPerPath.collect { case (_, Some(title), _, _) => title }
    val fileAccessibilities = dataPerPath.collect { case (_, _, Some(far), _) => far }
    val fileVisibilities = dataPerPath.collect { case (_, _, _, Some(fv)) => fv }
    val errors = ListBuffer[Throwable]()

    if (titles.size > 1) errors.append(ParseException(rowNum, s"FILE_TITLE defined multiple values for file '$file': ${ titles.mkString("[", ", ", "]") }"))
    if (fileAccessibilities.size > 1) errors.append(ParseException(rowNum, s"FILE_ACCESSIBILITY defined multiple values for file '$file': ${ fileAccessibilities.mkString("[", ", ", "]") }"))
    if (fileVisibilities.size > 1) errors.append(ParseException(rowNum, s"FILE_VISIBILITY defined multiple values for file '$file': ${ fileVisibilities.mkString("[", ", ", "]") }"))

    if (errors.nonEmpty) Failure(CompositeException(errors))
    else {
      (fileAccessibilities, fileVisibilities) match {
        case (List(as), List(vs)) if vs > as => Failure(ParseException(rowNum,
          s"FILE_VISIBILITY ($vs) is more restricted than FILE_ACCESSIBILITY ($as) for file '$file'. (User will potentially have access to an invisible file.)"))
        case _ => Success((file, FileDescriptor(titles.headOption, fileAccessibilities.headOption, fileVisibilities.headOption)))
      }
    }
  }

  def fileDescriptor(depositId: DepositId)(rowNum: => Int)(row: DepositRow): Option[Try[(File, Option[String], Option[FileAccessRights.Value], Option[FileAccessRights.Value])]] = {
    def collectErrors(rs: Option[Try[_]]*): Seq[Throwable] = {
      rs.collect { case Some(Failure(t)) => ParseException(rowNum, t.getMessage, t) }
    }

    val path = row.find("FILE_PATH").map(findRegularFile(depositId))
    val title = row.find("FILE_TITLE")
    val accessibility = fileAccessibility(rowNum)(row)
    val visibility = fileVisibility(rowNum)(row)

    (path, title, accessibility, visibility) match {
      case (None, None, None, None) => None
      case (None, _, a, v) =>
        val errors = collectErrors(a, v)
        Some(Failure(CompositeException(
          ParseException(rowNum, "FILE_TITLE, FILE_ACCESSIBILITY and FILE_VISIBILITY are only allowed if FILE_PATH is also given") +: errors)))
      case (Some(p), t, a, v) =>
        val errors = collectErrors(Some(p), a, v)
        if (errors.isEmpty) Some(Success((p.get, t, a.map(_.get), v.map(_.get)))) // _.get is safe here, because errors.isEmpty means that neither a nor v is a Failure.
        else Some(Failure(CompositeException(errors)))
    }
  }

  def fileAccessibility(rowNum: => Int)(row: DepositRow): Option[Try[FileAccessRights.Value]] = {
    row.find("FILE_ACCESSIBILITY")
      .map(acc => FileAccessRights.valueOf(acc)
        .map(Success(_))
        .getOrElse(Failure(ParseException(rowNum, s"Value '$acc' is not a valid file accessright"))))
  }

  def fileVisibility(rowNum: => Int)(row: DepositRow): Option[Try[FileAccessRights.Value]] = {
    row.find("FILE_VISIBILITY")
      .map(acc => FileAccessRights.valueOf(acc)
        .map(Success(_))
        .getOrElse(Failure(ParseException(rowNum, s"Value '$acc' is not a valid file visibility"))))
  }
}
