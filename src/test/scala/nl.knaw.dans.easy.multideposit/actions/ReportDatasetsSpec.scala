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
package nl.knaw.dans.easy.multideposit.actions

import java.util.UUID

import nl.knaw.dans.easy.multideposit.TestSupportFixture
import org.scalatest.BeforeAndAfterEach

class ReportDatasetsSpec extends TestSupportFixture with BeforeAndAfterEach {

  private val action = new ReportDatasets

  override def beforeEach(): Unit = {
    super.beforeEach()
    if (outputPathExplorer.reportFile.exists)
      outputPathExplorer.reportFile.delete()
  }

  "report" should "list the depositId, bagId and baseId for all deposits in a multi-deposit" in {
    val depositId1 = "deposit1"
    val depositId2 = "deposit2"

    val bagId1 = UUID.randomUUID()
    val bagId2 = UUID.randomUUID()

    val baseId1 = UUID.randomUUID()
    val baseId2 = UUID.randomUUID()

    val deposits = Seq(
      testInstructions1.toDeposit().copy(depositId = depositId1, bagId = bagId1, baseUUID = Some(baseId1)),
      testInstructions1.toDeposit().copy(depositId = depositId2, bagId = bagId2, baseUUID = Some(baseId2))
    )

    outputPathExplorer.reportFile.toJava shouldNot exist

    action.report(deposits)

    outputPathExplorer.reportFile.toJava should exist
    outputPathExplorer.reportFile.lines.toList should contain inOrderOnly(
      "DATASET,UUID,BASE-REVISION",
      s"$depositId1,$bagId1,$baseId1",
      s"$depositId2,$bagId2,$baseId2"
    )
  }

  it should "list use the bagId as baseId when this is not a new revision of an earlier dataset" in {
    val depositId1 = "deposit1"
    val depositId2 = "deposit2"

    val bagId1 = UUID.randomUUID()
    val bagId2 = UUID.randomUUID()

    val baseId1 = UUID.randomUUID()

    val deposits = Seq(
      testInstructions1.toDeposit().copy(depositId = depositId1, bagId = bagId1, baseUUID = Some(baseId1)),
      // baseUUID = None, causes bagId2 to be used in the CSV instead
      testInstructions1.toDeposit().copy(depositId = depositId2, bagId = bagId2, baseUUID = None)
    )

    outputPathExplorer.reportFile.toJava shouldNot exist

    action.report(deposits)

    outputPathExplorer.reportFile.toJava should exist
    outputPathExplorer.reportFile.lines.toList should contain inOrderOnly(
      "DATASET,UUID,BASE-REVISION",
      s"$depositId1,$bagId1,$baseId1",
      s"$depositId2,$bagId2,$bagId2"
    )
  }
}
