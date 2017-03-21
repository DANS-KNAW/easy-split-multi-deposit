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

import scala.util.{ Failure, Success, Try }
import nl.knaw.dans.lib.error.{ CompositeException, TraversableTryExtensions }
import nl.knaw.dans.lib.logging.DebugEnhancedLogging

import scala.annotation.tailrec
import scala.util.control.NonFatal

/*
  To future developers: this class is an Applicative Functor. It satisfies the law as defined in
    - https://wiki.haskell.org/Typeclassopedia#Laws
    - https://wiki.haskell.org/Typeclassopedia#Laws_2.
 */
trait Action[+T] extends DebugEnhancedLogging {

  private def logPreconditions(): Unit = {
    logger.info(s"Checking preconditions of ${getClass.getSimpleName} ...")
  }
  private def logExecute(): Unit = {
    logger.info(s"Executing action of ${getClass.getSimpleName} ...")
  }
  private def logRollback(): Unit = {
    logger.info(s"An error occurred. Rolling back action ${getClass.getSimpleName} ...")
  }
  private[Action] def innerCheckPreconditions: Try[Unit] = Try(logPreconditions()).flatMap(_ => checkPreconditions)
  private[Action] def innerExecute(): Try[T] = Try(logExecute()).flatMap(_ => execute())
  private[Action] def innerRollback(): Try[Unit] = Try(logRollback()).flatMap(_ => rollback())

  /**
   * Verifies whether all preconditions are met for this specific action.
   *
   * @return `Success` when all preconditions are met, `Failure` otherwise
   */
  protected def checkPreconditions: Try[Unit] = Success(())

  /**
   * Exectue the action.
   *
   * @return `Success` if the execution was successful, `Failure` otherwise
   */
  protected def execute(): Try[T]

  /**
   * Cleans up results of a previous call to run so that a new call to run will not fail because of those results.
   *
   * @return `Success` if the rollback was successful, `Failure` otherwise
   */
  protected def rollback(): Try[Unit] = Success(())

  /**
   * Run an action. First the precondition is checked. If it fails a `PreconditionsFailedException`
   * with a report is returned. Else, if the precondition succeed, the action is executed.
   * If this fails, the action is rolled back and a `ActionRunFailedException` with a report is returned.
   * If the execution was successful, `Success` is returned
   *
   * @return `Success` if the full execution was successful, `Failure` otherwise
   */
  def run(): Try[T] = {
    def runFailed(t: Throwable): Try[T] = {
      Failure(ActionRunFailedException(
        report = generateReport(
          header = "Errors in Multi-Deposit Instructions file:",
          throwable = t,
          footer = "The actions that were already performed, were rolled back."),
        cause = t
      ))
    }

    for {
      _ <- innerCheckPreconditions.recoverWith {
        case NonFatal(e) =>
          Failure(PreconditionsFailedException(
            report = generateReport(
              header = "Precondition failures:",
              throwable = e,
              footer = "Due to these errors in the preconditions, nothing was done."),
            cause = e))
      }
      t <- innerExecute().recoverWith {
        case e1@CompositeException(es) =>
          rollback() match {
            case Success(_) => runFailed(e1)
            case Failure(CompositeException(es2)) => runFailed(CompositeException(es ++ es2))
            case Failure(e2) => runFailed(CompositeException(es ++ List(e2)))
          }
        case NonFatal(e1) =>
          rollback() match {
            case Success(_) => runFailed(e1)
            case Failure(CompositeException(es2)) => runFailed(CompositeException(List(e1) ++ es2))
            case Failure(e2) => runFailed(CompositeException(List(e1, e2)))
          }
      }
    } yield t
  }

  private def generateReport(header: String = "", throwable: Throwable, footer: String = ""): String = {

    @tailrec
    def report(es: List[Throwable], rpt: List[String] = Nil): List[String] = {
      es match {
        case Nil => rpt
        case ActionException(row, msg, _) :: xs => report(xs, s" - row $row: $msg" :: rpt)
        case CompositeException(ths) :: xs => report(ths.toList ::: xs, rpt)
        case NonFatal(ex) :: xs => report(xs, s" - unexpected error: ${ex.getMessage}" :: rpt)
      }
    }

    header.toOption.fold("")(_ + "\n") +
      report(List(throwable)).reverse.mkString("\n") +
      footer.toOption.fold("")("\n" + _)
  }

  // fmap
  def map[S](f: T => S): Action[S] = Action(
    () => this.checkPreconditions,
    () => this.execute().map(f),
    () => this.rollback())

  /*
    Note that this operator will change the order of operations in the 'execute' phase.
    First 'other' is executed, then 'self'.
    The order in the preconditions and rollback phases is preserved.
   */
  // <*>
  def applyLeft[S, R](other: Action[S])(implicit ev: T <:< (S => R)): Action[R] = combineWith(other)((f, t) => f(t))

  // <**>
  def applyRight[S](other: Action[T => S]): Action[S] = combineWith(other)((t, f) => f(t))

  // <*
  def thenAnd[S](other: Action[S]): Action[T] = combineWith(other)((t, _) => t)

  // *>
  def andThen[S](other: Action[S]): Action[S] = combineWith(other)((_, s) => s)

  // liftA2
  def combineWith[S, R](other: Action[S])(f: (T, S) => R): Action[R] = Action.CombinedAction(this, other)(f)
}

object Action {
  def apply[T](precondition: () => Try[Unit] = () => Success(()),
            action: () => Try[T],
            undo: () => Try[Unit] = () => Success(())): Action[T] = new Action[T] {
    override protected def checkPreconditions: Try[Unit] = precondition()
    override protected def execute(): Try[T] = action()
    override protected def rollback(): Try[Unit] = undo()
  }

  def from[A](a: A): Action[A] = Action(action = () => Success(a))

  case class CombinedAction[X, Y, Z](left: Action[X], right: Action[Y])(f: (X, Y) => Z) extends Action[Z] {
    private var pastLeft = false

    override def run(): Try[Z] = {
      pastLeft = false
      super.run()
    }

    override def checkPreconditions: Try[Unit] = {
      List(left, right).map(_.checkPreconditions).collectResults.map(_ => ())
    }

    override def execute(): Try[Z] = {
      for {
        t <- left.execute()
        _ = pastLeft = true
        s <- right.execute()
      } yield f(t, s)
    }

    override def rollback(): Try[Unit] = {
      (if (pastLeft) List(right, left) else List(left))
        .map(_.rollback())
        .collectResults
        .map(_ => ())
    }
  }
}
