/**
 * Copyright (C) 2016 DANS - Data Archiving and Networked Services (info@dans.knaw.nl)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package nl.knaw.dans.easy.multideposit.actions

import java.io.File
import javax.naming.directory.{ Attributes, BasicAttribute, BasicAttributes }

import nl.knaw.dans.easy.multideposit.{ ActionException, Settings, UnitSpec, _ }
import nl.knaw.dans.lib.error.CompositeException
import org.scalamock.scalatest.MockFactory
import org.scalatest.{ BeforeAndAfter, BeforeAndAfterAll, PrivateMethodTester }

import scala.collection.mutable
import scala.util.{ Failure, Success }

class AddPropertiesToDepositSpec extends UnitSpec with BeforeAndAfter with BeforeAndAfterAll with MockFactory {

  val ldapMock: Ldap = mock[Ldap]
  implicit val settings = Settings(
    multidepositDir = new File(testDir, "md"),
    stagingDir = new File(testDir, "sd"),
    datamanager = "dm",
    ldap = ldapMock
  )
  val datasetID = "ds1"
  val dataset = mutable.HashMap(
    "DEPOSITOR_ID" -> List("dp1", "", "", "")
  )

  def mockLdapForDepositor(b: Boolean): Unit = {
    (ldapMock.query(_: String)(_: Attributes => Boolean)) expects ("dp1", *) returning Success(Seq(b))
  }

  before {
    new File(settings.stagingDir, s"md-$datasetID").mkdirs
  }

  override def afterAll: Unit = testDir.getParentFile.deleteDirectory()

  "checkPreconditions" should "succeed if the depositorID is in the dataset" in {
    mockLdapForDepositor(true)

    AddPropertiesToDeposit(1, (datasetID, dataset)).checkPreconditions shouldBe a[Success[_]]
  }

  it should "succeed if the depositorID column contains multiple but equal values" in {
    val dataset = mutable.HashMap(
      "DEPOSITOR_ID" -> List("dp1", "dp1", "dp1", "dp1")
    )
    mockLdapForDepositor(true)

    AddPropertiesToDeposit(1, (datasetID, dataset)).checkPreconditions shouldBe a[Success[_]]
  }

  it should "fail when the depositorID column is not in the dataset" in {
    val dataset = mutable.HashMap(
      "TEST_COLUMN" -> List("abc", "def")
    )

    inside(AddPropertiesToDeposit(1, (datasetID, dataset)).checkPreconditions) {
      case Failure(ActionException(_, message, _)) => message should include ("is not present")
    }
  }

  it should "fail when the depositorID column contains multiple different values" in {
    val dataset = mutable.HashMap(
      "DEPOSITOR_ID" -> List("dp1", "dp1", "dp2", "dp1")
    )

    inside(AddPropertiesToDeposit(1, (datasetID, dataset)).checkPreconditions) {
      case Failure(ActionException(_, message, _)) => message should include ("multiple distinct")
    }
  }

  it should "fail if ldap identifies the depositorID as not active" in {
    mockLdapForDepositor(false)

    inside(AddPropertiesToDeposit(1, (datasetID, dataset)).checkPreconditions) {
      case Failure(ActionException(_, message, _)) => message should include ("""depositor "dp1" is not an active user""")
    }
  }

  it should "fail if ldap does not return anything for the depositor" in {
    (ldapMock.query(_: String)(_: Attributes => Boolean)) expects ("dp1", *) returning Success(Seq.empty)

    inside(AddPropertiesToDeposit(1, (datasetID, dataset)).checkPreconditions) {
      case Failure(ActionException(_, message, _)) => message should include ("""DepositorID "dp1" is unknown""")
    }
  }

  it should "fail if ldap returns multiple values" in {
    (ldapMock.query(_: String)(_: Attributes => Boolean)) expects ("dp1", *) returning Success(Seq(true, true))

    inside(AddPropertiesToDeposit(1, (datasetID, dataset)).checkPreconditions) {
      case Failure(ActionException(_, message, _)) => message should include ("""multiple users with id "dp1"""")
    }
  }

  "execute" should "generate the properties file and write the properties in it" in {
    AddPropertiesToDeposit(1, (datasetID, dataset)).execute().map(f => f("dm@test.org")) shouldBe a[Success[_]]

    val props = stagingPropertiesFile(datasetID)
    props should exist

    val content = props.read()
    content should include ("state.label")
    content should include ("state.description")

    content should include ("depositor.userId=dp1")

    content should include ("datamanager.email=dm@test.org")
    content should include ("datamanager.userId=dm")
  }
}
