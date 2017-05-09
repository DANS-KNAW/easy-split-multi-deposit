package nl.knaw.dans.easy.multideposit

import nl.knaw.dans.easy.multideposit.model.MultiDepositKey

package object parser {

  type DatasetRow = Map[MultiDepositKey, String]
  type DatasetRows = Seq[DatasetRow]

  implicit class DatasetRowFind(val row: DatasetRow) extends AnyVal {
    def find(name: MultiDepositKey): Option[String] = row.get(name).filterNot(_.isBlank)
  }
}
