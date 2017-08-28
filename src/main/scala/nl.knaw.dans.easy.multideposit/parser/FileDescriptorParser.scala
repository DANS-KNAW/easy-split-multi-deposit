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

import java.nio.file.{ Files, Path }

import nl.knaw.dans.easy.multideposit.{ ParseException, Settings }
import nl.knaw.dans.easy.multideposit.model.{ DepositId, FileAccessRights, FileDescriptor }
import nl.knaw.dans.lib.error.{ CompositeException, TraversableTryExtensions }

import scala.util.{ Failure, Success, Try }

trait FileDescriptorParser {
  this: ParserUtils =>

  implicit val settings: Settings

  def extractFileDescriptors(rows: DepositRows, rowNum: Int, depositId: DepositId): Try[Map[Path, FileDescriptor]] = {
    extractList(rows)(fileDescriptor(depositId))
      .flatMap(_.groupBy { case (file, _, _) => file }
        .map((toFileDescriptor(rowNum) _).tupled)
        .collectResults
        .map(_.toMap))
  }

  private def toFileDescriptor(rowNum: => Int)(path: Path, dataPerPath: List[(Path, Option[String], Option[FileAccessRights.Value])]): Try[(Path, FileDescriptor)] = {
    val titles = dataPerPath.collect { case (_, Some(title), _) => title }
    val fars = dataPerPath.collect { case (_, _, Some(far)) => far }

    (titles, fars) match {
      case (Nil, Nil) => Success((path, FileDescriptor()))
      case (t :: Nil, Nil) => Success((path, FileDescriptor(Some(t))))
      case (Nil, f :: Nil) => Success((path, FileDescriptor(accessibility = Some(f))))
      case (t :: Nil, f :: Nil) => Success((path, FileDescriptor(Some(t), Some(f))))
      case (ts, fs) if fs.size <= 1 => Failure(ParseException(rowNum, s"FILE_TITLE defined multiple values for file '$path': ${ ts.mkString("[", ", ", "]") }"))
      case (ts, fs) if ts.size <= 1 => Failure(ParseException(rowNum, s"FILE_ACCESSIBILITY defined multiple values for file '$path': ${ fs.mkString("[", ", ", "]") }"))
      case (ts, fs) => Failure(new CompositeException(
        ParseException(rowNum, s"FILE_TITLE defined multiple values for file '$path': ${ ts.mkString("[", ", ", "]") }"),
        ParseException(rowNum, s"FILE_ACCESSIBILITY defined multiple values for file '$path': ${ fs.mkString("[", ", ", "]") }")))
    }
  }

  def fileDescriptor(depositId: DepositId)(rowNum: => Int)(row: DepositRow): Option[Try[(Path, Option[String], Option[FileAccessRights.Value])]] = {
    val path = row.find("FILE_PATH").map(findPath(depositId))
    val title = row.find("FILE_TITLE")
    val accessRights = fileAccessRight(rowNum)(row)

    (path, title, accessRights) match {
      case (Some(p), t, Some(Success(ar)))
        if Files.exists(p)
          && Files.isRegularFile(p) =>
        Some(Success((p, t, Some(ar))))
      case (Some(p), _, Some(Success(_)))
        if !Files.exists(p) =>
        Some(Failure(ParseException(rowNum, s"FILE_PATH '$p' does not exist")))
      case (Some(p), _, Some(Success(_))) =>
        Some(Failure(ParseException(rowNum, s"FILE_PATH '$p' is not a file")))
      case (Some(p), t, None)
        if Files.exists(p)
          && Files.isRegularFile(p) =>
        Some(Success((p, t, None)))
      case (Some(p), _, None)
        if !Files.exists(p) =>
        Some(Failure(ParseException(rowNum, s"FILE_PATH '$p' does not exist")))
      case (Some(p), _, None)
        if !Files.isRegularFile(p) =>
        Some(Failure(ParseException(rowNum, s"FILE_PATH '$p' is not a file")))
      case (Some(p), _, Some(Failure(e)))
        if Files.exists(p)
          && Files.isRegularFile(p) =>
        Some(Failure(e))
      case (Some(p), _, Some(Failure(e)))
        if !Files.exists(p) =>
        Some(Failure(new CompositeException(ParseException(rowNum, s"FILE_PATH '$p' does not exist"), e)))
      case (Some(p), _, Some(Failure(e)))
        if !Files.isRegularFile(p) =>
        Some(Failure(new CompositeException(ParseException(rowNum, s"FILE_PATH '$p' is not a file"), e)))
      case (None, Some(_), Some(Success(_))) =>
        Some(Failure(ParseException(rowNum, "FILE_TITLE and FILE_ACCESSIBILITY are not allowed, since FILE_PATH isn't given")))
      case (None, Some(_), Some(Failure(e))) =>
        Some(Failure(new CompositeException(ParseException(rowNum, "FILE_TITLE and FILE_ACCESSIBILITY are not allowed, since FILE_PATH isn't given"), e)))
      case (None, Some(_), None) =>
        Some(Failure(ParseException(rowNum, "FILE_TITLE is not allowed, since FILE_PATH isn't given")))
      case (None, None, Some(Success(_))) =>
        Some(Failure(ParseException(rowNum, "FILE_ACCESSIBILITY is not allowed, since FILE_PATH isn't given")))
      case (None, None, Some(Failure(e))) =>
        Some(Failure(new CompositeException(ParseException(rowNum, "FILE_ACCESSIBILITY is not allowed, since FILE_PATH isn't given"), e)))
      case (None, None, None) => None
    }
  }

  def fileAccessRight(rowNum: => Int)(row: DepositRow): Option[Try[FileAccessRights.Value]] = {
    row.find("FILE_ACCESSIBILITY")
      .map(acc => FileAccessRights.valueOf(acc)
        .map(Success(_))
        .getOrElse(Failure(ParseException(rowNum, s"Value '$acc' is not a valid file accessright"))))
  }
}

object FileDescriptorParser {
  def apply()(implicit ss: Settings): FileDescriptorParser = new FileDescriptorParser with ParserUtils {
    override val settings: Settings = ss
  }
}
