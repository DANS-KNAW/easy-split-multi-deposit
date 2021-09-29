/*
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

import better.files.File
import nl.knaw.dans.easy.multideposit.model.{ BagId, DepositId }

object PathExplorer {

  class PathExplorers(md: File, sd: File, od: File, report: File) extends InputPathExplorer with StagingPathExplorer with OutputPathExplorer {
    override val multiDepositDir: File = md
    override val stagingDir: File = sd
    override val outputDepositDir: File = od
    override val reportFile: File = report
  }

  trait InputPathExplorer {
    val multiDepositDir: File

    // mdDir/depositId/
    def depositDir(depositId: DepositId): File = {
      multiDepositDir / depositId
    }

    // mdDir/instructions.csv
    def instructionsFile: File = multiDepositDir / instructionsFileName
  }

  trait StagingPathExplorer {
    this: InputPathExplorer =>

    val stagingDir: File

    val bagDirName = "bag"
    val dataDirName = "data"
    val metadataDirName = "metadata"
    val datasetMetadataFileName = "dataset.xml"
    val fileMetadataFileName = "files.xml"
    val propsFileName = "deposit.properties"

    private def datasetDir(multiDepositDir: File, depositId: DepositId): String = {
      s"${ multiDepositDir.name }-$depositId"
    }

    // stagingDir/mdDir-depositId/
    def stagingDir(depositId: DepositId): File = {
      stagingDir / datasetDir(multiDepositDir, depositId)
    }

    // stagingDir/mdDir-depositId/bag/
    def stagingBagDir(depositId: DepositId): File = {
      stagingDir(depositId) / bagDirName
    }

    // stagingDir/mdDir-depositId/bag/data/
    def stagingBagDataDir(depositId: DepositId): File = {
      stagingBagDir(depositId) / dataDirName
    }

    // stagingDir/mdDir-depositId/bag/metadata/
    def stagingBagMetadataDir(depositId: DepositId): File = {
      stagingBagDir(depositId) / metadataDirName
    }

    // stagingDir/mdDir-depositId/deposit.properties
    def stagingPropertiesFile(depositId: DepositId): File = {
      stagingDir(depositId) / propsFileName
    }

    // stagingDir/mdDir-depositId/bag/metadata/dataset.xml
    def stagingDatasetMetadataFile(depositId: DepositId): File = {
      stagingBagMetadataDir(depositId) / datasetMetadataFileName
    }

    // stagingDir/mdDir-depositId/bag/metadata/files.xml
    def stagingFileMetadataFile(depositId: DepositId): File = {
      stagingBagMetadataDir(depositId) / fileMetadataFileName
    }
  }

  trait OutputPathExplorer {
    this: InputPathExplorer =>

    val outputDepositDir: File
    val reportFile: File

    // outputDepositDir/bagId/
    def outputDepositDir(bagId: BagId): File = {
      outputDepositDir / bagId.toString
    }
  }

  val instructionsFileName = "instructions.csv"

  def multiDepositInstructionsFile(baseDir: File): File = {
    baseDir / instructionsFileName
  }
}
