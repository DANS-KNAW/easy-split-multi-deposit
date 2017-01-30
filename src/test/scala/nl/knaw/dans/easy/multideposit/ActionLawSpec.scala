/**
 * Copyright (C) 2016 DANS - Data Archiving and Networked Services (info@dans.knaw.nl)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
