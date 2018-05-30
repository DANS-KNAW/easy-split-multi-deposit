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
import org.joda.time.DateTime
import org.scalatest.BeforeAndAfterEach

class CreateReportOfBaseUUIDsSpec extends TestSupportFixture with BeforeAndAfterEach {

  val action = new CreateReportOfBaseUUIDs
  val depositId = "depositId"
  val created = new DateTime()
  val base = Option(UUID.fromString("773dc53a-1cdb-47c4-992a-254a59b98376"))

  "createReportOfBaseUUIDs" should "create the report of UUIDs and BASE_UUIDs" in {

    //action.printBaseUUID(depositId, created, base) shouldBe Success()
  }
}
