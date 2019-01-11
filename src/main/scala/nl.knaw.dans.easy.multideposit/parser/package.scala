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
import cats.data.{ EitherNec, NonEmptyChain, ValidatedNec }
import cats.syntax.either._
import cats.syntax.validated._
import nl.knaw.dans.easy.multideposit.model.MultiDepositKey
import nl.knaw.dans.lib.string._

package object parser {

  case class DepositRow(rowNum: Int, content: Map[MultiDepositKey, String])
  type DepositRows = Seq[DepositRow]

  implicit class DatasetRowFind(val row: DepositRow) extends AnyVal {
    def find(name: MultiDepositKey): Option[String] = row.content.get(name).filterNot(_.isBlank)
  }

  type FailFast[T] = Either[ParseError, T]
  type FailFastNec[T] = EitherNec[ParseError, T]
  type Validated[T] = ValidatedNec[ParseError, T]

  implicit class ValidatedSyntax[T](val t: T) extends AnyVal {
    def toValidated: Validated[T] = t.validNec[ParseError]
  }

  implicit class FailFastSyntax[T](val either: FailFast[T]) extends AnyVal {
    def toValidated: Validated[T] = either.toValidatedNec
  }

  private[parser] sealed abstract class ParserError
  private[parser] case class EmptyInstructionsFileError(file: File) extends ParserError
  private[parser] case class ParseError(row: Int, message: String) extends ParserError {
    def toInvalid[T]: Validated[T] = this.invalidNec

    def chained: NonEmptyChain[ParseError] = NonEmptyChain.one(this)
  }

  // TODO remove extends Exception
  // kept here for now to be conform with the rest of the application
  case class ParseFailed(report: String) extends Exception(report)
}
