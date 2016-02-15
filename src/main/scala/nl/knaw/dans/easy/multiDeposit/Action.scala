package nl.knaw.dans.easy.multiDeposit

import scala.util.Try

/**
  * An action to be performed by Process SIP. It provides three methods that can be invoked to verify
  * the feasibility of the action, to perform the action and - if necessary - to roll back the action.
  *
  * @param row the row on which this action is executed
  */
abstract class Action(row: String) {

  /**
    * Verifies whether all preconditions are met for this specific action.
    *
    * @return `Success` when all preconditions are met, `Failure` otherwise
    */
  def checkPreconditions: Try[Unit]

  /**
    * Exectue the action.
    *
    * @return `Success` if the execution was successfull, `Failure` otherwise
    */
  def run(): Try[Unit]

  /**
    * Cleans up results of a previous call to run so that a new call to run will not fail because of those results.
    *
    * @return TODO
    */
  def rollback(): Try[Unit]
}
