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

import java.nio.file.Path

import org.scalatest.matchers.{ MatchResult, Matcher }

import scala.io.Source
import scala.language.postfixOps
import scala.xml._

/** Does not dump the full file but just the searched content if it is not found.
 *
 * See also <a href="http://www.scalatest.org/user_guide/using_matchers#usingCustomMatchers">CustomMatchers</a> */
trait CustomMatchers {
  class ContentMatcher(content: String) extends Matcher[Path] {
    def apply(left: Path): MatchResult = {
      def trimLines(s: String): String = s.split("\n").map(_.trim).mkString("\n")

      MatchResult(
        trimLines(Source.fromFile(left.normalize().toFile).mkString).contains(trimLines(content)),
        s"$left did not contain: $content",
        s"$left contains $content"
      )
    }
  }
  def containTrimmed(content: String) = new ContentMatcher(content)

  class EqualTrimmedMatcher(right: Iterable[Node]) extends Matcher[Iterable[Node]] {
    override def apply(left: Iterable[Node]): MatchResult = {
      MatchResult(
        left.zip(right).forall { case (l, r) =>
          val pp = new PrettyPrinter(160, 2)
          XML.loadString(pp.format(l)) == XML.loadString(pp.format(r))
        },
        s"$left did not equal $right",
        s"$left did equal $right"
      )
    }
  }
  def equalTrimmed(right: Iterable[Node]) = new EqualTrimmedMatcher(right)

  class ContainAllTrimmed(right: Iterable[Node]) extends Matcher[Iterable[Node]] {
    override def apply(left: Iterable[Node]): MatchResult = {
      MatchResult(
        {
          left.size == right.size && left.map(l => right.exists(l ==)).forall(true ==)
        },
        // TODO showing the diffs should be done better, but I don't know yet how to use the IntelliJ supported solution to get a nice diff
        // need to look deeper into that.
        s"$left did not contain the same elements as $right: ${ missingLeft(left, right) }; ${ missingRight(left, right) }",
        s"$left did contain the same elements as $right"
      )
    }

    private def missingLeft(left: Iterable[Node], right: Iterable[Node]) = {
      val diff = right.toList diff left.toList
      diff match {
        case Nil => ""
        case _ => s"Missing in left: ${ diff.mkString("[", ", ", "]") }"
      }
    }

    private def missingRight(left: Iterable[Node], right: Iterable[Node]) = {
      val diff = left.toList diff right.toList
      diff match {
        case Nil => ""
        case _ => s"Missing in right: ${ diff.mkString("[", ", ", "]") }"
      }
    }
  }
  def containAllNodes(right: Iterable[Node]) = new ContainAllTrimmed(right)
}
