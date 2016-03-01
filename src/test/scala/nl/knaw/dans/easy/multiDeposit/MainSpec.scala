/**
 * Copyright (C) 2015-2016 DANS - Data Archiving and Networked Services (info@dans.knaw.nl)
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

import org.scalamock.scalatest.MockFactory
import rx.lang.scala.Observable
import rx.lang.scala.observers.TestSubscriber

import scala.util.{Failure, Success}

class MainSpec extends UnitSpec with MockFactory {

  abstract class MockedAction extends Action(42)

  "checkActionPreconditions" should "succeed if the preconditions of are met" in {
    val m1 = mock[MockedAction]
    val m2 = mock[MockedAction]

    m1.checkPreconditions _ expects () once() returning Success(())
    m2.checkPreconditions _ expects () once() returning Success(())

    m1.run _ expects () never()
    m1.rollback _ expects () never()
    m2.run _ expects () never()
    m2.rollback _ expects () never()

    val testSubscriber = TestSubscriber[Action]
    Main.checkActionPreconditions(Observable.just(m1, m2)).subscribe(testSubscriber)

    testSubscriber.assertValues(m1, m2)
    testSubscriber.assertNoErrors
    testSubscriber.assertCompleted
    testSubscriber.assertUnsubscribed
  }

  it should "return all actions and then fail when multiple preconditions fail" in {
    val m1 = mock[MockedAction]
    val m2 = mock[MockedAction]
    val m3 = mock[MockedAction]
    val m4 = mock[MockedAction]

    m1.checkPreconditions _ expects () once() returning Success(())
    m2.checkPreconditions _ expects () once() returning Failure(new ActionException(6, "foo"))
    m3.checkPreconditions _ expects () once() returning Success(())
    m4.checkPreconditions _ expects () once() returning Failure(new ActionException(1, "bar"))

    m1.run _ expects () never()
    m1.rollback _ expects () never()
    m2.run _ expects () never()
    m2.rollback _ expects () never()
    m3.run _ expects () never()
    m3.rollback _ expects () never()
    m4.run _ expects () never()
    m4.rollback _ expects () never()

    val testSubscriber = TestSubscriber[Action]
    Main.checkActionPreconditions(Observable.just(m1, m2, m3, m4))
      .subscribe(testSubscriber)

    testSubscriber.assertNoValues
    testSubscriber.assertError(classOf[Exception])
    testSubscriber.assertNotCompleted
    testSubscriber.assertUnsubscribed
  }

  it should "generate an error report when multiple preconditions fail" in {
    val m1 = mock[MockedAction]
    val m2 = mock[MockedAction]
    val m3 = mock[MockedAction]
    val m4 = mock[MockedAction]

    m1.checkPreconditions _ expects () once() returning Success(())
    m2.checkPreconditions _ expects () once() returning Failure(new ActionException(6, "foo"))
    m3.checkPreconditions _ expects () once() returning Success(())
    m4.checkPreconditions _ expects () once() returning Failure(new ActionException(1, "bar"))

    m1.run _ expects () never()
    m1.rollback _ expects () never()
    m2.run _ expects () never()
    m2.rollback _ expects () never()
    m3.run _ expects () never()
    m3.rollback _ expects () never()
    m4.run _ expects () never()
    m4.rollback _ expects () never()

    val testSubscriber = TestSubscriber[Object]
    Main.checkActionPreconditions(Observable.just(m1, m2, m3, m4))
      .onErrorResumeNext(e => Observable.just(e.getMessage))
      .subscribe(testSubscriber)

    testSubscriber.assertValue("Precondition failures:\n - row 1: bar\n - row 6: foo")
    testSubscriber.assertNoErrors
    testSubscriber.assertCompleted
    testSubscriber.assertUnsubscribed
  }

  "runActions" should "succeed if all actions are successfull in running" in {
    val m1 = mock[MockedAction]
    val m2 = mock[MockedAction]

    m1.run _ expects () once() returning Success(())
    m2.run _ expects () once() returning Success(())

    m1.rollback _ expects () never()
    m2.rollback _ expects () never()

    val testSubscriber = TestSubscriber[Action]
    Main.runActions(Observable.just(m1, m2)).subscribe(testSubscriber)

    testSubscriber.assertValues(m1, m2)
    testSubscriber.assertNoErrors
    testSubscriber.assertCompleted
    testSubscriber.assertUnsubscribed
  }

  it should "rollback when it fails, return the action and return after that" in {
    val exception = new Exception("foo")

    val m1 = mock[MockedAction]
    val m2 = mock[MockedAction]
    val m3 = mock[MockedAction]

    m1.run _ expects () once() returning Success(())
    m2.run _ expects () once() returning Failure(exception)
    m3.run _ expects () never()

    m1.rollback _ expects () once()
    m2.rollback _ expects () once()
    m3.rollback _ expects () never()

    val testSubscriber = TestSubscriber[Action]
    Main.runActions(Observable.just(m1, m2, m3)).subscribe(testSubscriber)

    testSubscriber.assertValues(m1, m2)
    testSubscriber.assertError(exception)
    testSubscriber.assertNotCompleted
    testSubscriber.assertUnsubscribed
  }
}
