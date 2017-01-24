package nl.knaw.dans.easy.multideposit

import org.scalacheck.Arbitrary
import org.scalacheck.Arbitrary._
import org.scalatest.prop.PropertyChecks
import org.scalatest.{ Matchers, PropSpec }

import scala.util.{ Failure, Success, Try }

class ActionLawSpec extends PropSpec with PropertyChecks with Matchers {

  implicit def arbTry[T](implicit a: Arbitrary[T]): Arbitrary[Try[T]] = {
    Arbitrary(arbitrary[Option[T]].map(_.map(Success(_)).getOrElse(Failure(new Exception("failure")))))
  }

  implicit def arbAction: Arbitrary[Action] = {
    Arbitrary(for {
      pre <- arbitrary[Try[Unit]]
      exe <- arbitrary[Try[Unit]]
      undo <- arbitrary[Try[Unit]]
    } yield Action(() => pre, () => exe, () => undo))
  }

  property("action - semigroup associativity") {
    forAll { (a1: Action, a2: Action, a3: Action) =>
      a1.compose(a2).compose(a3) shouldBe a1.compose(a2.compose(a3))
    }
  }

  property("action - semigroup combined") {
    forAll { (a1: Action, a2: Action, a3: Action, a4: Action) =>
      a1.compose(a2).compose(a3.compose(a4)) shouldBe a1.compose(a2).compose(a3).compose(a4)
    }
  }
}
