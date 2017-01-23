package nl.knaw.dans.easy.multideposit

import org.scalamock.scalatest.MockFactory

import scala.util.{ Failure, Success }

class ActionSpec extends UnitSpec with MockFactory {

  "run" should "succeed if the precondition and execute both return Success" in {
    val action = mock[Action]

    action.checkPreconditions _ expects () once() returning Success(())
    action.execute _ expects () once() returning Success(())
    action.rollback _ expects () never() returning Success(())
    action.run _ expects () once() returning Action(() => action.checkPreconditions, action.execute, action.rollback).run

    action.run shouldBe a[Success[_]]
  }

  it should "fail if the precondition fails" in {
    val action = mock[Action]

    action.checkPreconditions _ expects () once() returning Failure(ActionException(1, "pre"))
    action.execute _ expects () never()
    action.rollback _ expects () never()
    action.run _ expects () once() returning Action(() => action.checkPreconditions, action.execute, action.rollback).run

    inside(action.run) {
      case Failure(PreconditionsFailedException(report)) => report should include ("1: pre")
    }
  }

  it should "fail if the execute fails" in {
    val action = mock[Action]

    action.checkPreconditions _ expects () once() returning Success(())
    action.execute _ expects () once() returning Failure(ActionException(1, "exe"))
    action.rollback _ expects () once() returning Success(())
    action.run _ expects () once() returning Action(() => action.checkPreconditions, action.execute, action.rollback).run

    inside(action.run) {
      case Failure(ActionRunFailedException(report)) => report should include ("1: exe")
    }
  }

  it should "succeed if the precondition and execute succeed but the rollback fails (never called)" in {
    val action = mock[Action]

    action.checkPreconditions _ expects () once() returning Success(())
    action.execute _ expects () once() returning Success(())
    action.rollback _ expects () never() returning Failure(ActionException(1, "undo"))
    action.run _ expects () once() returning Action(() => action.checkPreconditions, action.execute, action.rollback).run

    action.run shouldBe a[Success[_]]
  }

  it should "fail if both the execute and rollback fail" in {
    val action = mock[Action]

    action.checkPreconditions _ expects () once() returning Success(())
    action.execute _ expects () once() returning Failure(ActionException(1, "exe"))
    action.rollback _ expects () once() returning Failure(ActionException(1, "undo"))
    action.run _ expects () once() returning Action(() => action.checkPreconditions, action.execute, action.rollback).run

    inside(action.run) {
      case Failure(ActionRunFailedException(report)) => report should (include ("1: exe") and include ("1: undo"))
    }
  }

  "compose" should "succeed if everything succeeds" in {
    val m1 = mock[Action]
    val m2 = mock[Action]

    m1.checkPreconditions _ expects () once() returning Success(())
    m1.execute _ expects () once() returning Success(())
    m1.rollback _ expects () never()

    m2.checkPreconditions _ expects () once() returning Success(())
    m2.execute _ expects () once() returning Success(())
    m2.rollback _ expects () never()

    Action.empty.compose(m1).compose(m2).run shouldBe a[Success[_]]
  }

  it should "fail when the first precondition fails" in {
    val m1 = mock[Action]
    val m2 = mock[Action]

    m1.checkPreconditions _ expects () once() returning Failure(ActionException(1, "pre"))
    m1.execute _ expects () never()
    m1.rollback _ expects () never()

    m2.checkPreconditions _ expects () once() returning Success(())
    m2.execute _ expects () never()
    m2.rollback _ expects () never()

    inside(Action.empty.compose(m1).compose(m2).run) {
      case Failure(PreconditionsFailedException(report)) => report should include ("row 1: pre")
    }
  }

  it should "fail when the second precondition fails" in {
    val m1 = mock[Action]
    val m2 = mock[Action]

    m1.checkPreconditions _ expects () once() returning Success(())
    m1.execute _ expects () never()
    m1.rollback _ expects () never()

    m2.checkPreconditions _ expects () once() returning Failure(ActionException(2, "pre"))
    m2.execute _ expects () never()
    m2.rollback _ expects () never()

    inside(Action.empty.compose(m1).compose(m2).run) {
      case Failure(PreconditionsFailedException(report)) => report should include ("row 2: pre")
    }
  }

  it should "fail when both preconditions fail" in {
    val m1 = mock[Action]
    val m2 = mock[Action]

    m1.checkPreconditions _ expects () once() returning Failure(ActionException(1, "pre"))
    m1.execute _ expects () never()
    m1.rollback _ expects () never()

    m2.checkPreconditions _ expects () once() returning Failure(ActionException(2, "pre"))
    m2.execute _ expects () never()
    m2.rollback _ expects () never()

    inside(Action.empty.compose(m1).compose(m2).run) {
      case Failure(PreconditionsFailedException(report)) => report should (include ("row 1: pre") and include ("row 2: pre"))
    }
  }

  it should "fail when the preconditions succeed, but the first run fails" in {
    val m1 = mock[Action]
    val m2 = mock[Action]

    m1.checkPreconditions _ expects () once() returning Success(())
    m1.execute _ expects () once() returning Failure(ActionException(1, "exe"))
    m1.rollback _ expects () once() returning Success(())

    m2.checkPreconditions _ expects () once() returning Success(())
    m2.execute _ expects () never()
    m2.rollback _ expects () never()

    inside(Action.empty.compose(m1).compose(m2).run) {
      case Failure(ActionRunFailedException(report)) => report should include ("row 1: exe")
    }
  }

  it should "fail when the preconditions succeed, but the second run fails" in {
    val m1 = mock[Action]
    val m2 = mock[Action]

    m1.checkPreconditions _ expects () once() returning Success(())
    m1.execute _ expects () once() returning Success(())
    m1.rollback _ expects () once() returning Success(())

    m2.checkPreconditions _ expects () once() returning Success(())
    m2.execute _ expects () once() returning Failure(ActionException(2, "exe"))
    m2.rollback _ expects () once() returning Success(())

    inside(Action.empty.compose(m1).compose(m2).run) {
      case Failure(ActionRunFailedException(report)) => report should include ("row 2: exe")
    }
  }

  it should "fail when the preconditions succeed, but both the second run and rollback fail" in {
    val m1 = mock[Action]
    val m2 = mock[Action]

    m1.checkPreconditions _ expects () once() returning Success(())
    m1.execute _ expects () once() returning Success(())
    m1.rollback _ expects () once() returning Failure(ActionException(1, "undo"))

    m2.checkPreconditions _ expects () once() returning Success(())
    m2.execute _ expects () once() returning Failure(ActionException(2, "exe"))
    m2.rollback _ expects () once() returning Failure(ActionException(2, "undo"))

    inside(Action.empty.compose(m1).compose(m2).run) {
      case Failure(ActionRunFailedException(report)) => report should (include ("row 1: undo") and include ("row 2: exe") and include ("row 2: undo"))
    }
  }
}
