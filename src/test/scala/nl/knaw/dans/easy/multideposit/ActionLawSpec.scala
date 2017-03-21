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
import org.scalatest.{ Inside, Matchers, PropSpec }

import scala.util.Try

class ActionLawSpec extends PropSpec with PropertyChecks with Matchers with Inside with CustomMatchers {

  implicit def arbAction[T](implicit t: Arbitrary[T]): Arbitrary[Action[T]] = {
    Arbitrary(for {
      pre <- arbitrary[Try[Unit]]
      exe <- arbitrary[Try[T]]
      undo <- arbitrary[Try[Unit]]
    } yield Action(() => pre, () => exe, () => undo))
  }

  // fmap id == id
  property("action - functor identity") {
    forAll { a: Action[Int] =>
      a.map(identity) should equalAction(a)
    }
  }

  // fmap (g . h) == (fmap g) . (fmap h)
  property("action - functor composition") {
    forAll { (a: Action[Int], f: Int => String, g: String => Long) =>
      a.map(g compose f) should equalAction(a.map(f).map(g))
    }
  }

  // pure id <*> v == v
  property("action - applicative identity") {
    forAll { a: Action[Int] =>
      Action.from[Int => Int](identity).applyLeft(a) should equalAction(a)
    }
  }

  // pure f <*> pure x == pure (f x)
  property("action - applicative homomorphism") {
    forAll { (i: Int, f: Int => String) =>
      Action.from(f).applyLeft(Action.from(i)) should equalAction(Action.from(f(i)))
    }
  }

  // u <*> pure y == pure ($ y) <*> u
  property("action - applicative interchange") {
    forAll { (i: Int, actionF: Action[Int => String]) =>
      actionF.applyLeft(Action.from(i)) should equalAction(Action.from[(Int => String) => String](_(i)).applyLeft(actionF))
    }
  }

  // fmap v f == pure ($ f) <*> v
  property("action - applicative map") {
    forAll { (action: Action[Int], f: Int => String) =>
      action.map(f) should equalAction(Action.from(f).applyLeft(action))
    }
  }

  // u <*> (v <*> w) == ((pure (.) <*> u) <*> v) <*> w
  property("action - applicative composition") {
    forAll { (actionIntToString: Action[Int => String], actionLongToInt: Action[Long => Int], appLong: Action[Long]) =>
      val left = actionIntToString.applyLeft(actionLongToInt.applyLeft(appLong))

      val pure = Action.from[(Int => String) => (Long => Int) => Long => String](_.compose)
      val f = pure.applyLeft(actionIntToString)
      val g = f.applyLeft(actionLongToInt)
      val right = g.applyLeft(appLong)

      left should equalAction(right)
    }
  }
}
