package nl.knaw.dans.easy.multideposit

import scala.language.implicitConversions

package object parser {

  type MultiDepositKey = String
  type DatasetID = String
  type DepositorId = String

  type DatasetRow = Map[MultiDepositKey, String]
  type DatasetRows = Seq[DatasetRow]

  // inspired by http://stackoverflow.com/questions/28223692/what-is-the-optimal-way-not-using-scalaz-to-type-require-a-non-empty-list
  type NonEmptyList[A] = ::[A]

  implicit def listToNEL[A](list: List[A]): ::[A] = {
    assert(list.nonEmpty)
    ::(list.head, list.tail)
  }

  implicit class DatasetRowFind(val row: DatasetRow) extends AnyVal {
    def find(name: MultiDepositKey): Option[String] = row.get(name).filterNot(_.isBlank)
  }
}
