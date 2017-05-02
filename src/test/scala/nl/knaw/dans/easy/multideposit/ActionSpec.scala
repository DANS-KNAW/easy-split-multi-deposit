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

import java.util.concurrent.atomic.AtomicBoolean

import scala.language.implicitConversions
import scala.util.{ Failure, Success, Try }

class ActionSpec extends UnitSpec {

  class TestAction[T](precondition: Try[Unit],
                      execute: Try[T],
                      rollback: Try[Unit]) {
    private val pre = new AtomicBoolean()
    private val exe = new AtomicBoolean()
    private val undo = new AtomicBoolean()
    val action: Action[Unit, T] = Action(
      precondition = () => { pre.set(true); precondition },
      action = _ => { exe.set(true); execute },
      undo = () => { undo.set(true); rollback }
    )

    def visited: (Boolean, Boolean, Boolean) = (pre.get(), exe.get(), undo.get())
  }
  implicit def testActionIsAction[T](testAction: TestAction[T]): Action[Unit, T] = testAction.action

  "run" should "succeed if the precondition and execute both return Success" in {
    val action = new TestAction(
      precondition = Success(()),
      execute = Success(42),
      rollback = Success(()))

    inside(action.run(())) {
      case Success(x) => x shouldBe 42
    }
    action.visited shouldBe(true, true, false)
  }

  it should "fail if the precondition fails" in {
    val action = new TestAction(
      precondition = Failure(ActionException(1, "pre")),
      execute = Success(42),
      rollback = Success(()))

    inside(action.run(())) {
      case Failure(PreconditionsFailedException(report, _)) => report shouldBe
        """Precondition failures:
          | - row 1: pre
          |Due to these errors in the preconditions, nothing was done.""".stripMargin
    }
    action.visited shouldBe(true, false, false)
  }

  it should "fail if the execute fails" in {
    val action = new TestAction(
      precondition = Success(()),
      execute = Failure(ActionException(1, "exe")),
      rollback = Success(()))

    inside(action.run(())) {
      case Failure(ActionRunFailedException(report, _)) => report shouldBe
        """Errors during processing:
          | - row 1: exe
          |The actions that were already performed, were rolled back.""".stripMargin
    }
    action.visited shouldBe(true, true, true)
  }

  it should "succeed if the precondition and execute succeed but the rollback fails (never called)" in {
    val action = new TestAction(
      precondition = Success(()),
      execute = Success(42),
      rollback = Failure(ActionException(1, "undo")))

    inside(action.run(())) {
      case Success(x) => x shouldBe 42
    }
    action.visited shouldBe(true, true, false)
  }

  it should "fail if both the execute and rollback fail" in {
    val action = new TestAction(
      precondition = Success(()),
      execute = Failure(ActionException(1, "exe")),
      rollback = Failure(ActionException(1, "undo")))

    inside(action.run(())) {
      case Failure(ActionRunFailedException(report, _)) => report shouldBe
        """Errors during processing:
          | - row 1: exe
          | - row 1: undo
          |The actions that were already performed, were rolled back.""".stripMargin
    }
    action.visited shouldBe(true, true, true)
  }

  "combine" should "succeed if everything succeeds" in {
    val m1 = new TestAction[Unit](
      precondition = Success(()),
      execute = Success(()),
      rollback = Success(()))
    val m2 = new TestAction[Unit](
      precondition = Success(()),
      execute = Success(()),
      rollback = Success(()))
    val m3 = new TestAction[Unit](
      precondition = Success(()),
      execute = Success(()),
      rollback = Success(()))

    m1.combine(m2).combine(m3).run(()) shouldBe a[Success[_]]
    m1.visited shouldBe(true, true, false)
    m2.visited shouldBe(true, true, false)
    m3.visited shouldBe(true, true, false)
  }

  it should "fail when the first precondition fails" in {
    val m1 = new TestAction[Unit](
      precondition = Failure(ActionException(1, "pre")),
      execute = Success(()),
      rollback = Success(()))
    val m2 = new TestAction[Unit](
      precondition = Success(()),
      execute = Success(()),
      rollback = Success(()))
    val m3 = new TestAction[Unit](
      precondition = Success(()),
      execute = Success(()),
      rollback = Success(()))

    inside(m1.combine(m2).combine(m3).run(())) {
      case Failure(PreconditionsFailedException(report, _)) => report shouldBe
        """Precondition failures:
          | - row 1: pre
          |Due to these errors in the preconditions, nothing was done.""".stripMargin
    }
    m1.visited shouldBe(true, false, false)
    m2.visited shouldBe(true, false, false)
    m3.visited shouldBe(true, false, false)
  }

  it should "fail when the second precondition fails" in {
    val m1 = new TestAction[Unit](
      precondition = Success(()),
      execute = Success(()),
      rollback = Success(()))
    val m2 = new TestAction[Unit](
      precondition = Failure(ActionException(2, "pre")),
      execute = Success(()),
      rollback = Success(()))
    val m3 = new TestAction[Unit](
      precondition = Success(()),
      execute = Success(()),
      rollback = Success(()))

    inside(m1.combine(m2).combine(m3).run(())) {
      case Failure(PreconditionsFailedException(report, _)) => report shouldBe
        """Precondition failures:
          | - row 2: pre
          |Due to these errors in the preconditions, nothing was done.""".stripMargin
    }
    m1.visited shouldBe(true, false, false)
    m2.visited shouldBe(true, false, false)
    m3.visited shouldBe(true, false, false)
  }

  it should "fail when multiple preconditions fail" in {
    val m1 = new TestAction[Unit](
      precondition = Failure(ActionException(1, "pre")),
      execute = Success(()),
      rollback = Success(()))
    val m2 = new TestAction[Unit](
      precondition = Failure(ActionException(2, "pre")),
      execute = Success(()),
      rollback = Success(()))
    val m3 = new TestAction[Unit](
      precondition = Success(()),
      execute = Success(()),
      rollback = Success(()))

    inside(m1.combine(m2).combine(m3).run(())) {
      case Failure(PreconditionsFailedException(report, _)) => report shouldBe
        """Precondition failures:
          | - row 1: pre
          | - row 2: pre
          |Due to these errors in the preconditions, nothing was done.""".stripMargin
    }
    m1.visited shouldBe(true, false, false)
    m2.visited shouldBe(true, false, false)
    m3.visited shouldBe(true, false, false)
  }

  it should "fail when the preconditions succeed, but the first run fails" in {
    val m1 = new TestAction[Unit](
      precondition = Success(()),
      execute = Failure(ActionException(1, "exe")),
      rollback = Success(()))
    val m2 = new TestAction[Unit](
      precondition = Success(()),
      execute = Success(()),
      rollback = Success(()))
    val m3 = new TestAction[Unit](
      precondition = Success(()),
      execute = Success(()),
      rollback = Success(()))

    inside(m1.combine(m2).combine(m3).run(())) {
      case Failure(ActionRunFailedException(report, _)) => report shouldBe
        """Errors during processing:
          | - row 1: exe
          |The actions that were already performed, were rolled back.""".stripMargin
    }
    m1.visited shouldBe(true, true, true)
    m2.visited shouldBe(true, false, false)
    m3.visited shouldBe(true, false, false)
  }

  it should "fail when the preconditions succeed, but the second run fails" in {
    val m1 = new TestAction[Unit](
      precondition = Success(()),
      execute = Success(()),
      rollback = Success(()))
    val m2 = new TestAction[Unit](
      precondition = Success(()),
      execute = Failure(ActionException(2, "exe")),
      rollback = Success(()))
    val m3 = new TestAction[Unit](
      precondition = Success(()),
      execute = Success(()),
      rollback = Success(()))

    inside(m1.combine(m2).combine(m3).run(())) {
      case Failure(ActionRunFailedException(report, _)) => report shouldBe
        """Errors during processing:
          | - row 2: exe
          |The actions that were already performed, were rolled back.""".stripMargin
    }
    m1.visited shouldBe(true, true, true)
    m2.visited shouldBe(true, true, true)
    m3.visited shouldBe(true, false, false)
  }

  it should "fail when the preconditions succeed, but both the second run and rollback fail, as well as the first rollback" in {
    val m1 = new TestAction[Unit](
      precondition = Success(()),
      execute = Success(()),
      rollback = Failure(ActionException(1, "undo")))
    val m2 = new TestAction[Unit](
      precondition = Success(()),
      execute = Failure(ActionException(2, "exe")),
      rollback = Failure(ActionException(2, "undo")))
    val m3 = new TestAction[Unit](
      precondition = Success(()),
      execute = Success(()),
      rollback = Success(()))

    inside(m1.combine(m2.combine(m3)).run(())) {
      case Failure(ActionRunFailedException(report, _)) => report shouldBe
        """Errors during processing:
          | - row 2: exe
          | - row 2: undo
          | - row 1: undo
          |The actions that were already performed, were rolled back.""".stripMargin
    }
    m1.visited shouldBe(true, true, true)
    m2.visited shouldBe(true, true, true)
    m3.visited shouldBe(true, false, false)
  }
}
