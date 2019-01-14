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

import cats.data.Validated
import cats.data.Validated.{ Invalid, Valid }
import org.scalactic._
import org.scalatest.exceptions.{ StackDepthException, TestFailedException }

import scala.language.implicitConversions

trait ValidatedValues {

  implicit def convertValidatedToValuable[E, A](validated: Validated[E, A])(implicit pos: source.Position): ValidatedValueable[E, A] = new ValidatedValueable(validated, pos)

  class ValidatedValueable[E, A](validated: Validated[E, A], pos: source.Position) {
    def validValue: A = {
      validated match {
        case Valid(a) => a
        case Invalid(_) => throw new TestFailedException((_: StackDepthException) => Some("The Validated on which validValue was invoked was not defined as a Valid."), None, pos)
      }
    }

    def invalidValue: E = {
      validated match {
        case Valid(_) => throw new TestFailedException((_: StackDepthException) => Some("The Validated on which invalidValue was invoked was not defined as a Invalid."), None, pos)
        case Invalid(e) => e
      }
    }
  }
}

object ValidatedValues extends ValidatedValues
