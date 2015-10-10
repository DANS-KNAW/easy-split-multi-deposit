package nl.knaw.dans.easy.ps

import scala.util.Try

/**
 * An action to be performed by Process SIP. It provides three methods that can be invoked to verify
 * the feasibility of the action, to perform the action and - if necessary - to roll back the action.
 */
abstract class Action(row: String) {
  /**
   * Verifies that the Action will most probably succeed.
   */
  def checkPreconditions: Try[Unit]

  /**
   * Runs the action.
   */
  def run(): Try[Unit]

  /**
   * Cleans up results of a previous call to run so that a new call to run will not fail because of those results.
   */
  def rollback(): Try[Unit]
}

