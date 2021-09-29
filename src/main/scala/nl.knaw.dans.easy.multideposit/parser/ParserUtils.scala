/*
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

import java.net.{ URI, URISyntaxException }

import better.files.File
import cats.data.{ NonEmptyList, Validated }
import cats.instances.list._
import cats.syntax.option._
import cats.syntax.traverse._
import cats.syntax.validated._
import nl.knaw.dans.easy.multideposit.PathExplorer.InputPathExplorer
import nl.knaw.dans.easy.multideposit.model.DepositId
import nl.knaw.dans.easy.multideposit.parser.Headers.Header
import nl.knaw.dans.lib.string._
import org.joda.time.DateTime

trait ParserUtils {
  this: InputPathExplorer =>

  def extractExactlyOne(rowNum: Int, name: Header, rows: DepositRows): Validated[String] = {
    rows.flatMap(_.find(name)).distinct match {
      case Seq() => ParseError(rowNum, s"There should be one non-empty value for $name").toInvalid
      case Seq(t) => t.toValidated
      case ts => ParseError(rowNum, s"Only one row is allowed to contain a value for the column '$name'. Found: ${ ts.mkString("[", ", ", "]") }").toInvalid
    }
  }

  def extractAtLeastOne(rowNum: Int, name: Header, rows: DepositRows): Validated[NonEmptyList[String]] = {
    rows.flatMap(_.find(name)).distinct match {
      case Seq() => ParseError(rowNum, s"There should be at least one non-empty value for $name").toInvalid
      case Seq(head, tail @ _*) => NonEmptyList.of(head, tail: _*).toValidated
    }
  }

  def extractAtMostOne(rowNum: Int, name: Header, rows: DepositRows): Validated[Option[String]] = {
    rows.flatMap(_.find(name)).distinct match {
      case Seq() => none.toValidated
      case Seq(t) => t.some.toValidated
      case ts => ParseError(rowNum, s"At most one row is allowed to contain a value for the column '$name'. Found: ${ ts.mkString("[", ", ", "]") }").toInvalid
    }
  }

  def extractList[T](rows: DepositRows)(f: DepositRow => Option[Validated[T]]): Validated[List[T]] = {
    rows.flatMap(row => f(row)).toList.sequence
  }

  def extractList(rows: DepositRows, name: Header): List[String] = {
    rows.flatMap(_.find(name)).toList
  }

  def checkValidChars(value: String, rowNum: => Int, column: => Header): Validated[String] = {
    val invalidCharacters = "[^a-zA-Z0-9_-]".r.findAllIn(value).toSeq.distinct
    if (invalidCharacters.isEmpty) value.toValidated
    else ParseError(rowNum, s"The column '$column' contains the following invalid characters: ${ invalidCharacters.mkString("{", ", ", "}") }").toInvalid
  }

  def date(rowNum: => Int, columnName: => Header)(s: String): Validated[DateTime] = {
    Validated.catchOnly[IllegalArgumentException] { DateTime.parse(s) }
      .leftMap(_ => ParseError(rowNum, s"$columnName value '$s' does not represent a date"))
      .toValidatedNec
  }

  val validURIschemes = List("http", "https")

  def uri(rowNum: => Int, columnName: => Header)(s: String): Validated[URI] = {
    Validated.catchOnly[URISyntaxException] { new URI(s) }
      .leftMap(_ => ParseError(rowNum, s"$columnName value '$s' is not a valid URI"))
      .andThen {
        case uri if validURIschemes contains uri.getScheme => uri.valid
        case _ => ParseError(rowNum, s"$columnName value '$s' is a valid URI but doesn't have one of the accepted protocols: ${ validURIschemes.mkString("{", ", ", "}") }").invalid
      }
      .toValidatedNec
  }

  def missingRequired(row: DepositRow, required: Header*): ParseError = {
    val blankRequired = row.content.collect { case (key, value) if value.isBlank && required.contains(key) => key }
    val missingColumns = required.toSet.diff(row.content.keySet)
    val missing = blankRequired.toSet ++ missingColumns
    require(missing.nonEmpty, "the list of missing elements is supposed to be non-empty")

    missing.toList match {
      case value :: Nil => ParseError(row.rowNum, s"Missing value for: $value")
      case values => ParseError(row.rowNum, s"Missing value(s) for: ${ values.mkString("[", ", ", "]") }")
    }
  }

  /**
   * Returns the absolute file for the given `path`. If the input is correct, `path` is relative
   * to the deposit it is in.
   *
   * If both options do not suffice, the path is just wrapped in a `File`.
   *
   * @param path the path to a file, as provided by the user input
   * @return the absolute path to this file, if it exists
   */
  def findRegularFile(depositId: DepositId, rowNum: => Int)(path: String): Validated[File] = {
    val file = depositDir(depositId) / path

    file match {
      case f if f.isRegularFile => f.toValidated
      case f if f.exists => ParseError(rowNum, s"path '$path' exists, but is not a regular file").toInvalid
      case _ => ParseError(rowNum, s"unable to find path '$path'").toInvalid
    }
  }
}
