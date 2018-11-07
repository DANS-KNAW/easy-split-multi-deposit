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
import org.apache.commons.configuration.PropertiesConfiguration
import org.scalatest.BeforeAndAfterEach

import scala.collection.JavaConverters._
import scala.util.Success

class AddPropertiesToDepositSpec extends TestSupportFixture with BeforeAndAfterEach {

  private val depositId = "ds1"
  private val datamanagerId = "dm"
  private val datamanagerEmail = "dm@test.org"
  private val action = new AddPropertiesToDeposit

  override def beforeEach(): Unit = {
    val path = stagingDir / s"sd-$depositId"
    if (path.exists) path.delete()
    path.createDirectories()
  }

  "addDepositProperties" should "generate the properties file and write the properties in it" in {
    val uuid = UUID.randomUUID()
    action.addDepositProperties(testInstructions1.copy(audioVideo = AudioVideo()).toDeposit().copy(bagId = uuid), datamanagerId, datamanagerEmail) shouldBe a[Success[_]]

    val props = stagingPropertiesFile(testInstructions1.depositId)
    props.toJava should exist

    val resultProps = new PropertiesConfiguration {
      setDelimiterParsingDisabled(true)
      load(props.toJava)
    }

    resultProps.getKeys.asScala.toList should {
      contain only(
        "bag-store.bag-id",
        "creation.timestamp",
        "state.label",
        "state.description",
        "depositor.userId",
        "curation.datamanager.email",
        "curation.datamanager.userId",
        "curation.required",
        "curation.performed",
        "identifier.doi.registered"
      ) and contain noneOf(
        "springfield.domain",
        "springfield.user",
        "springfield.collection",
        "springfield.playmode",
      )
    }

    resultProps.getString("bag-store.bag-id") shouldBe uuid.toString
    resultProps.getString("depositor.userId") shouldBe "ruimtereiziger1"
    resultProps.getString("curation.datamanager.email") shouldBe datamanagerEmail
    resultProps.getString("curation.datamanager.userId") shouldBe datamanagerId
    resultProps.getString("curation.required") shouldBe "yes"
    resultProps.getString("curation.performed") shouldBe "yes"
    resultProps.getString("identifier.doi.registered") shouldBe "no"
  }

  it should "generate the properties file with springfield fields and write the properties in it" in {
    val uuid = UUID.randomUUID()

    action.addDepositProperties(testInstructions1.toDeposit().copy(bagId = uuid), datamanagerId, datamanagerEmail) shouldBe a[Success[_]]

    val props = stagingPropertiesFile(testInstructions1.depositId)
    props.toJava should exist

    val resultProps = new PropertiesConfiguration {
      setDelimiterParsingDisabled(true)
      load(props.toJava)
    }

    resultProps.getKeys.asScala.toList should {
      contain only(
        "bag-store.bag-id",
        "creation.timestamp",
        "state.label",
        "state.description",
        "depositor.userId",
        "curation.datamanager.email",
        "curation.datamanager.userId",
        "curation.required",
        "curation.performed",
        "springfield.domain",
        "springfield.user",
        "springfield.collection",
        "springfield.playmode",
        "identifier.doi.registered",
      )
    }

    resultProps.getString("bag-store.bag-id") shouldBe uuid.toString
    resultProps.getString("depositor.userId") shouldBe "ruimtereiziger1"
    resultProps.getString("curation.datamanager.email") shouldBe datamanagerEmail
    resultProps.getString("curation.datamanager.userId") shouldBe datamanagerId
    resultProps.getString("curation.required") shouldBe "yes"
    resultProps.getString("curation.performed") shouldBe "yes"
    resultProps.getString("springfield.domain") shouldBe "dans"
    resultProps.getString("springfield.user") shouldBe "janvanmansum"
    resultProps.getString("springfield.collection") shouldBe "Jans-test-files"
    resultProps.getString("springfield.playmode") shouldBe "menu"
    resultProps.getString("identifier.doi.registered") shouldBe "no"
  }
}
