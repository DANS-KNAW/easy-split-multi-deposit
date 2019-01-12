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
