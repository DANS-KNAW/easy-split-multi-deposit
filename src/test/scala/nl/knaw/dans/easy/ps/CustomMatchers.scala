package nl.knaw.dans.easy.ps

import org.scalatest.matchers._
import org.scalatest.words.ResultOfATypeInvocation

import scala.util.{Success, Failure, Try}

/** See also <a href="http://www.scalatest.org/user_guide/using_matchers#usingCustomMatchers">CustomMatchers</a> */
trait CustomMatchers {

  class ActionExceptionMatcher(expectedRow: String = null, expectedMessage: String) extends Matcher[Try[Unit]] {

    def apply(left: Try[Unit]) = {

      val expected = s"on row $expectedRow with ActionException having a message matching: $expectedMessage"
      val actualRow = left match {
        case Failure(ActionException(row, msg: String)) => row
        case _ => ""
      }
      def actualMessage = left match {
        case Failure(ActionException(row, msg: String)) => msg
        case _ => ""
      }
      def failureMessage = left match {
        case Failure(ActionException(row: String, msg: String)) => s"failed on row=$actualRow with ActionException having a message: $actualMessage"
        case Failure(e) => s"failed with $e"// includes message
        case Success(_) => "succeeded"
      }
      MatchResult(
        actualMessage.matches(s"$expectedMessage") && actualRow.matches(expectedRow),
        s"did not fail $expected but $failureMessage",
        s"failed $expected"
      )
    }
  }
  /** usage example: Action.XXX should failWithActionExceptionMatching ("1",msg=".*YYY.*") */
  def failWithActionExceptionMatching(row: String, msg: String) = new ActionExceptionMatcher(row, msg)

  class ExceptionMatcher(expectedException: ResultOfATypeInvocation[_], expectedMessage: String*) extends Matcher[Try[Unit]] {

    def apply(left: Try[Unit]) = {

      val expected = s"$expectedException having a message matching [$expectedMessage]"
      val thrown = left match { case Failure(e) => e case _=>""}
      def failureMessage = left match {
        case Failure(e) => s"failed with $e"//string includes message
        case Success(_) => "succeeded"
      }
      def actualMessage = Option(thrown.asInstanceOf[Throwable].getMessage) match{case Some(msg)=>msg case _=>""}
      val containsMessageFragments: Seq[Boolean] = for {m<-expectedMessage} yield {actualMessage.lastIndexOf(m)>=0}
      MatchResult(
        // TODO replace == by something like instanceOf
        thrown.getClass == expectedException.clazz && (!containsMessageFragments.contains(false)),
        s"did not fail with $expected but $failureMessage",
        s"failed with $expected"
      )
    }
  }
  /** usage example: Action.XXX should failWith (a[Throwable],"some message fragment", "another optional fragment) */
  def failWith(exception: ResultOfATypeInvocation[_], messageFragment: String*) = new ExceptionMatcher(exception, messageFragment: _*)
}

// Make them easy to import with:
// import CustomMatchers._
object CustomMatchers extends CustomMatchers
