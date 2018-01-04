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
package nl.knaw.dans.easy.multideposit2

import java.nio.file.Path

import nl.knaw.dans.easy.multideposit.model.DepositId

object PathExplorer {

  trait InputPathExplorer {
    def multiDepositDir: Path

    val instructionsFileName = "instructions.csv"

    // mdDir/depositId/
    def depositDir(depositId: DepositId): Path = {
      multiDepositDir.resolve(depositId)
    }

    // mdDir/instructions.csv
    def instructionsFile: Path = multiDepositDir.resolve(instructionsFileName)
  }

  trait StagingPathExplorer {
    this: InputPathExplorer =>

    def stagingDir: Path

    val bagDirName = "bag"
    val dataDirName = "data"
    val metadataDirName = "metadata"
    val datasetMetadataFileName = "dataset.xml"
    val fileMetadataFileName = "files.xml"
    val propsFileName = "deposit.properties"

    // stagingDir/mdDir-depositId/
    def stagingDir(depositId: DepositId): Path = {
      stagingDir.resolve(datasetDir(multiDepositDir, depositId))
    }

    // stagingDir/mdDir-depositId/bag/
    def stagingBagDir(depositId: DepositId): Path = {
      stagingDir(depositId).resolve(bagDirName)
    }

    // stagingDir/mdDir-depositId/bag/data/
    def stagingBagDataDir(depositId: DepositId): Path = {
      stagingBagDir(depositId).resolve(dataDirName)
    }

    // stagingDir/mdDir-depositId/bag/metadata/
    def stagingBagMetadataDir(depositId: DepositId): Path = {
      stagingBagDir(depositId).resolve(metadataDirName)
    }

    // stagingDir/mdDir-depositId/deposit.properties
    def stagingPropertiesFile(depositId: DepositId): Path = {
      stagingDir(depositId).resolve(propsFileName)
    }

    // stagingDir/mdDir-depositId/bag/metadata/dataset.xml
    def stagingDatasetMetadataFile(depositId: DepositId): Path = {
      stagingBagMetadataDir(depositId).resolve(datasetMetadataFileName)
    }

    // stagingDir/mdDir-depositId/bag/metadata/files.xml
    def stagingFileMetadataFile(depositId: DepositId): Path = {
      stagingBagMetadataDir(depositId).resolve(fileMetadataFileName)
    }
  }

  trait OutputPathExplorer {
    this: InputPathExplorer =>

    def outputDepositDir: Path

    // outputDepositDir/mdDir-depositId/
    def outputDepositDir(depositId: DepositId): Path = {
      outputDepositDir.resolve(datasetDir(multiDepositDir, depositId))
    }
  }

  private def datasetDir(multiDepositDir: Path, depositId: DepositId): String = {
    s"${ multiDepositDir.getFileName }-$depositId"
  }
}
