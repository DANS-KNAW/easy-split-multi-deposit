package nl.knaw.dans.easy.multideposit.actions

import nl.knaw.dans.easy.multideposit.PathExplorer.OutputPathExplorer
import nl.knaw.dans.easy.multideposit.model.Deposit

import scala.util.Try

class ReportDatasets {

  def report(deposits: Seq[Deposit])(implicit output: OutputPathExplorer): Try[Unit] = Try {
    // TODO implement
    println(output.reportFile)
  }
}
