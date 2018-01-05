package nl.knaw.dans.easy.multideposit2.actions

import java.nio.file.{ Files, Path }
import java.util.Collections

import gov.loc.repository.bagit.creator.BagCreator
import gov.loc.repository.bagit.domain.{ Metadata => BagitMetadata }
import gov.loc.repository.bagit.hash.StandardSupportedAlgorithms
import gov.loc.repository.bagit.verify.FileCountAndTotalSizeVistor
import nl.knaw.dans.easy.multideposit.FileExtensions
import nl.knaw.dans.easy.multideposit2.PathExplorer.{ InputPathExplorer, StagingPathExplorer }
import nl.knaw.dans.easy.multideposit2.model.DepositId
import org.joda.time.DateTime
import org.joda.time.format.ISODateTimeFormat

import scala.util.Try

trait AddBagToDeposit {
  this: InputPathExplorer with StagingPathExplorer =>

  def addBagToDeposit(depositId: DepositId, created: DateTime): Try[Unit] = Try {
    val inputDir = depositDir(depositId)
    val stageDir = stagingBagDir(depositId)

    val metadata = new BagitMetadata {
      add("Created", created.toString(ISODateTimeFormat.dateTime()))
    }

    if (Files.exists(inputDir)) {
      inputDir copyDir stageDir
      metadata.add("Bag-Size", formatSize(calculateSizeOfPath(inputDir)))
    }
    else {
      metadata.add("Bag-Size", formatSize(0L))
    }

    BagCreator.bagInPlace(stageDir, Collections.singletonList(StandardSupportedAlgorithms.SHA1), true, metadata)
  }

  private def calculateSizeOfPath(dir: Path): Long = {
    val visitor = new FileCountAndTotalSizeVistor

    Files.walkFileTree(dir, visitor)

    visitor.getTotalSize
  }

  private val kb: Double = Math.pow(2, 10)
  private val mb: Double = Math.pow(2, 20)
  private val gb: Double = Math.pow(2, 30)
  private val tb: Double = Math.pow(2, 40)

  private def formatSize(octets: Long): String = {
    def approximate(octets: Long): (String, Double) = {
      octets match {
        case o if o < mb => ("KB", kb)
        case o if o < gb => ("MB", mb)
        case o if o < tb => ("GB", gb)
        case _ => ("TB", tb)
      }
    }

    val (unit, div) = approximate(octets)
    val size = octets / div
    val sizeString = f"$size%1.1f"
    val string = if (sizeString endsWith ".0") size.toInt.toString
                 else sizeString

    s"$string $unit"
  }
}
