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

import org.scalacheck.Arbitrary
import org.scalacheck.Arbitrary._
import org.scalatest.prop.PropertyChecks
import org.scalatest.{ Inside, Matchers, PropSpec }

import scala.language.{ implicitConversions, reflectiveCalls }
import scala.util.{ Failure, Success, Try }

class ActionLawSpec extends PropSpec with PropertyChecks with Matchers with Inside with CustomMatchers {

  implicit def arbAction[T, S](implicit ts: Arbitrary[T => Try[S]]): Arbitrary[Action[T, S]] = {
    Arbitrary(for {
      pre <- arbitrary[Try[Unit]]
      exe <- arbitrary[T => Try[S]]
      undo <- arbitrary[Try[Unit]]
    } yield Action[T, S](() => pre, t => exe(t), () => undo))
  }

  property("action - category composition") {
    forAll { (i: Int, f: Int => Try[String], g: String => Try[Long]) =>
      Action(action = f).combine(Action(action = g)).run(i) shouldBe Action[Int, Long](action = f(_).flatMap(g)).run(i)
    }
  }

  property("action - semigroup associativity") {
    forAll { (i: Int, a1: Action[Int, String], a2: Action[String, Long], a3: Action[Long, Double]) =>
      compare(
        a1.combine(a2).combine(a3).run(i),
        a1.combine(a2.combine(a3)).run(i))
    }
  }

  property("action - semigroup combined") {
    forAll { (i: Int, a1: Action[Int, String], a2: Action[String, Long], a3: Action[Long, Double], a4: Action[Double, String]) =>
      compare(
        a1.combine(a2).combine(a3.combine(a4)).run(i),
        a1.combine(a2).combine(a3).combine(a4).run(i))
    }
  }

  private def compare[T](left: Try[T], right: Try[T]) = {
    inside(left, right) {
      case (Success(l), Success(r)) => l shouldBe r
      case (Failure(l), Failure(r)) =>
        l shouldBe a[r.type]
        l.getMessage shouldBe r.getMessage
    }
  }
}
