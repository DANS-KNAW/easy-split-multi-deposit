package nl.knaw.dans.easy.multiDeposit

import org.scalamock.scalatest.MockFactory
import rx.lang.scala.Observable
import rx.lang.scala.observers.TestSubscriber

import scala.util.{Failure, Success}

class MainSpec extends UnitSpec with MockFactory {

  abstract class MockedAction extends Action("mocked action")

  "checkActionPreconditions" should "succeed if the preconditions of are met" in {
    val m1 = mock[MockedAction]
    val m2 = mock[MockedAction]

    m1.checkPreconditions _ expects () once() returning Success()
    m2.checkPreconditions _ expects () once() returning Success()

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

    m1.checkPreconditions _ expects () once() returning Success()
    m2.checkPreconditions _ expects () once() returning Failure(new ActionException(6, "foo"))
    m3.checkPreconditions _ expects () once() returning Success()
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

    m1.checkPreconditions _ expects () once() returning Success()
    m2.checkPreconditions _ expects () once() returning Failure(new ActionException(6, "foo"))
    m3.checkPreconditions _ expects () once() returning Success()
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

    m1.run _ expects () once() returning Success()
    m2.run _ expects () once() returning Success()

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

    m1.run _ expects () once() returning Success()
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
