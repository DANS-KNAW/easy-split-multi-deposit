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

import better.files.File
import cats.data.{ NonEmptyChain, ValidatedNec }
import cats.syntax.validated._
import nl.knaw.dans.easy.multideposit.parser.Headers.Header
import nl.knaw.dans.lib.string._

package object parser {

  case class DepositRow(rowNum: Int, content: Map[Header, String])
  type DepositRows = Seq[DepositRow]

  implicit class DatasetRowFind(val row: DepositRow) extends AnyVal {
    def find(name: Header): Option[String] = row.content.get(name).filterNot(_.isBlank)
  }

  type Validated[T] = ValidatedNec[ParserError, T]

  implicit class ValidatedSyntax[T](val t: T) extends AnyVal {
    def toValidated: Validated[T] = t.validNec[ParserError]
  }

  private[parser] sealed abstract class ParserError {
    def toInvalid[T]: Validated[T] = this.invalidNec[T]

    def chained: NonEmptyChain[ParserError] = NonEmptyChain.one(this)
  }
  private[parser] case class EmptyInstructionsFileError(file: File) extends ParserError
  private[parser] case class ParseError(row: Int, message: String) extends ParserError
}
