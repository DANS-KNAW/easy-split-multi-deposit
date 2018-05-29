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
import nl.knaw.dans.easy.multideposit.model.AudioVideo
import org.joda.time.DateTime
import org.scalatest.BeforeAndAfterEach

import scala.util.Success

class AddPropertiesToDepositSpec extends TestSupportFixture with BeforeAndAfterEach {

  private val depositId = "ds1"
  private val datamanagerId = "dm"
  private val action = new AddPropertiesToDeposit

  override def beforeEach(): Unit = {
    val path = stagingDir / s"sd-$depositId"
    if (path.exists) path.delete()
    path.createDirectories()
  }

  "addDepositProperties" should "generate the properties file and write the properties in it" in {
    //val y = new DateTime()
    val y = DateTime.parse("2015-05-19")
    val z = UUID.fromString("1de3f841-0f0d-048b-b3db-4b03ad4834d7")
    action.addDepositProperties(testInstructions1.copy(audioVideo = AudioVideo()).toDeposit(), datamanagerId, "dm@test.org", depositId, y, Some(z)) shouldBe a[Success[_]]

    val props = stagingPropertiesFile(testInstructions1.depositId)
    props.toJava should exist

    props.contentAsString should {
      include("creation.timestamp") and
        include("state.label") and
        include("state.description") and
        include(s"depositor.userId=${ testInstructions1.depositorUserId }") and
        include("datamanager.email=dm@test.org") and
        include("datamanager.userId=dm") and
        not include "springfield.domain" and
        not include "springfield.user" and
        not include "springfield.collection" and
        not include "springfield.playmode"
    }
  }

  it should "generate the properties file with springfield fields and write the properties in it" in {
    //val y = new DateTime()
    val y = DateTime.parse("2015-05-19")
    val z = UUID.fromString("1de3f841-0f0d-048b-b3db-4b03ad4834d7")
    action.addDepositProperties(testInstructions1.toDeposit(), datamanagerId, "dm@test.org", depositId, y, Some(z)) shouldBe a[Success[_]]

    val props = stagingPropertiesFile(testInstructions1.depositId)
    props.toJava should exist

    props.contentAsString should {
      include("creation.timestamp") and
        include("state.label") and
        include("state.description") and
        include("depositor.userId=ruimtereiziger1") and
        include("datamanager.email=dm@test.org") and
        include("datamanager.userId=dm") and
        include("springfield.domain=dans") and
        include("springfield.user=janvanmansum") and
        include("springfield.collection=Jans-test-files") and
        include("springfield.playmode=menu") and
        include regex "bag-store.bag-id=[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}"
    }
  }
}
