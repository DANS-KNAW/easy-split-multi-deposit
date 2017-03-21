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

import nl.knaw.dans.easy.multideposit.Action.CombinedAction

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
  protected[Action] def innerCheckPreconditions: Try[Unit] = Try(logPreconditions()).flatMap(_ => checkPreconditions)
  protected[Action] def innerExecute(): Try[T] = Try(logExecute()).flatMap(_ => execute())
  protected[Action] def innerRollback(): Try[Unit] = Try(logRollback()).flatMap(_ => rollback())

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
    def reportFailure(t: Throwable): Try[T] = {
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
            case Success(_) => reportFailure(e1)
            case Failure(CompositeException(es2)) => reportFailure(CompositeException(es ++ es2))
            case Failure(e2) => reportFailure(CompositeException(es ++ List(e2)))
          }
        case NonFatal(e1) =>
          rollback() match {
            case Success(_) => reportFailure(e1)
            case Failure(CompositeException(es2)) => reportFailure(CompositeException(List(e1) ++ es2))
            case Failure(e2) => reportFailure(CompositeException(List(e1, e2)))
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

  /**
   * Return an `Action` that applies `f` to the output of this `execute`.
   *
   * @param f the transformation function to be applied
   * @tparam S the output type
   * @return an `Action` that returns an element of type `S`
   */
  def map[S](f: T => S): Action[S] = Action(
    () => this.checkPreconditions,
    () => this.execute().map(f),
    () => this.rollback())

  val x = 1 +: List(2)

  /**
   * Return an `Action` that applies the function in this to the value in `other` and returns the
   * resulting value. Note that this operator changes the order of operations in the 'execute' phase.
   * Since the function argument needs to be calculated before it can be applied to the function,
   * the `other` is executed first, followed by this `Action`. The order in precondition evaluation
   * and rollback is however preserved!
   *
   * A mnemonic for `applyLeft` vs `applyRight` is: the ''left'' or ''right'' signifies on which
   * side of the operator the ''function'' is
   *
   * @param other the `Action` containing the value to be applied to this function
   * @param ev the evidence that `T` is a function `S => R`
   * @tparam S the return type of the `other` action and the input type of the function in this `Action`
   * @tparam R the return type of this operator
   * @return an `Action` that applies the value on the right to the function on the left
   */
  def applyLeft[S, R](other: Action[S])(implicit ev: T <:< (S => R)): Action[R] = combineWith(other)((f, t) => f(t))

  /**
   * Return an `Action` that applies the function in `other` to the value in this action and returns
   * the resulting value.
   *
   * A mnemonic for `applyLeft` vs `applyRight` is: the ''left'' or ''right'' signifies on which
   * side of the operator the ''function'' is
   *
   * @param other the `Action` containing the value to be applied to this function
   * @tparam S the return type of this operator
   * @return an `Action` that applies the value on the left to the function on the right
   */
  def applyRight[S](other: Action[T => S]): Action[S] = combineWith(other)((t, f) => f(t))

  /**
   * Create an `Action` that sequentially executes this and `other` actions and returns the
   * output of this action afterwards. The result of `other` is discarded.
   *
   * @param other the other action
   * @tparam S the return type of `other`
   * @return an `Action` that executes both this and `other` and returns the result of this
   */
  def thenAnd[S](other: Action[S]): Action[T] = combineWith(other)((t, _) => t)

  /**
   * Create an `Action` that sequentially executes this and `other` actions and returns the
   * output of the `other` action afterwards. The result of this is discarded.
   *
   * @param other the other action
   * @tparam S the return type of `other` and this operator
   * @return an `Action` that executes both this and `other` and returns the result of `other`
   */
  def andThen[S](other: Action[S]): Action[S] = combineWith(other)((_, s) => s)

  /**
   * Create an `Action` that executes both this and `other` actions and combines the output
   * of both actions using the combinator `f`.
   *
   * @param other the other action
   * @param f the combinator function
   * @tparam S the return type of `other`
   * @tparam R the return type of this operator
   * @return an `Action` that executes both this and `other` and returns their combined result
   */
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

  /**
   * Lift a value into an `Action`. Note that only the `execute` is affected by this and that the
   * preconditions and rollback always return a `Success`.
   *
   * @param a the value to be lifted
   * @tparam A the type of `Action`
   * @return the `Action` in which the value is lifted
   */
  def from[A](a: A): Action[A] = Action(action = () => Success(a))

  case class CombinedAction[X, Y, Z](left: Action[X], right: Action[Y])(f: (X, Y) => Z) extends Action[Z] {
    private var pastLeft = false

    override def run(): Try[Z] = {
      pastLeft = false
      super.run()
    }

    override def innerCheckPreconditions: Try[Unit] = checkPreconditions
    override def innerExecute(): Try[Z] = execute()
    override def innerRollback(): Try[Unit] = rollback()

    override protected def checkPreconditions: Try[Unit] = {
      List(left, right).map(_.innerCheckPreconditions).collectResults.map(_ => ())
    }

    override protected def execute(): Try[Z] = {
      for {
        t <- left.innerExecute()
        _ = pastLeft = true
        s <- right.innerExecute()
      } yield f(t, s)
    }

    override protected  def rollback(): Try[Unit] = {
      (if (pastLeft) List(right, left) else List(left))
        .map(_.innerRollback())
        .collectResults
        .map(_ => ())
    }
  }
}
