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

class AddPropertiesToDepositSpec extends UnitSpec with BeforeAndAfter with MockFactory with PrivateMethodTester {

  val ldapMock: Ldap = mock[Ldap]
  implicit val settings = Settings(
    multidepositDir = new File(testDir, "md"),
    outputDepositDir = new File(testDir, "dd"),
    datamanager = "dm",
    ldap = ldapMock
  )
  val datasetID = "ds1"
  val dataset = mutable.HashMap(
    "DEPOSITOR_ID" -> List("dp1", "", "", "")
  )

  private val correctDatamanagerAttrs = createDatamanagerAttributes()

  /**
   * Default creates correct BasicAttributes
   */
  def createDatamanagerAttributes(state: String = "ACTIVE",
                                  roles: Seq[String] = Seq("USER","ARCHIVIST"),
                                  mail: String = "dm@test.org"): BasicAttributes = {

    val a = new BasicAttributes()
    a.put("dansState", state)
    a.put({
      val r = new BasicAttribute("easyRoles")
      roles.foreach(r.add)
      r
    })
    a.put("mail", mail)
    a
  }

  def mockLdapForDatamanager(attrs: Attributes): Unit = {
    (ldapMock.query(_: String)(_: Attributes => Attributes)) expects ("dm", *) returning Success(Seq(attrs))
  }

  def mockLdapForDepositor(b: Boolean): Unit = {
    (ldapMock.query(_: String)(_: Attributes => Boolean)) expects ("dp1", *) returning Success(Seq(b))
  }

  before {
    new File(settings.outputDepositDir, s"md-$datasetID").mkdirs
    // force datamanagerEmailaddress to be retrieved from LDAP for each test
    AddPropertiesToDeposit.invokePrivate(PrivateMethod[Unit]('resetDatamanagerEmailaddress)())
  }

  "checkPreconditions" should "succeed if the depositorID is in the dataset and has one value and the datamanager email can be retrieved" in {
    mockLdapForDepositor(true)
    mockLdapForDatamanager(correctDatamanagerAttrs)

    AddPropertiesToDeposit(1, (datasetID, dataset)).checkPreconditions shouldBe a[Success[_]]
  }

  it should "succeed if the depositorID column contains multiple but equal values" in {
    val dataset = mutable.HashMap(
      "DEPOSITOR_ID" -> List("dp1", "dp1", "dp1", "dp1")
    )
    mockLdapForDepositor(true)
    mockLdapForDatamanager(correctDatamanagerAttrs)

    AddPropertiesToDeposit(1, (datasetID, dataset)).checkPreconditions shouldBe a[Success[_]]
  }

  it should "fail if ldap does not return anything for the datamanager" in {
    mockLdapForDepositor(true)
    (ldapMock.query(_: String)(_: Attributes => Attributes)) expects ("dm", *) returning Success(Seq.empty)

    inside(AddPropertiesToDeposit(1, (datasetID, dataset)).checkPreconditions) {
      case Failure(CompositeException(es)) =>
        es should have size 1
        val ActionException(_, message, _) :: Nil = es
        message should include ("""The datamanager "dm" is unknown""")
    }
  }

  it should "fail when the datamanager is not an active user" in {
    val nonActiveDatamanagerAttrs = createDatamanagerAttributes(state = "BLOCKED")

    mockLdapForDepositor(true)
    mockLdapForDatamanager(nonActiveDatamanagerAttrs)

    inside(AddPropertiesToDeposit(1, (datasetID, dataset)).checkPreconditions) {
      case Failure(CompositeException(es)) =>
        es should have size 1
        val ActionException(_, message, _) :: Nil = es
        message should include ("not an active user")
    }
  }

  it should "fail when the datamanager is not an achivist" in {
    val nonArchivistDatamanagerAttrs = createDatamanagerAttributes(roles = Seq("USER"))

    mockLdapForDepositor(true)
    mockLdapForDatamanager(nonArchivistDatamanagerAttrs)

    inside(AddPropertiesToDeposit(1, (datasetID, dataset)).checkPreconditions) {
      case Failure(CompositeException(es)) =>
        es should have size 1
        val ActionException(_, message, _) :: Nil = es
        message should include ("is not an archivist")
    }
  }

  it should "fail when the datamanager has no email" in {
    val nonEmailDatamanagerAttrs = createDatamanagerAttributes(mail = "")

    mockLdapForDepositor(true)
    mockLdapForDatamanager(nonEmailDatamanagerAttrs)

    inside(AddPropertiesToDeposit(1, (datasetID, dataset)).checkPreconditions) {
      case Failure(CompositeException(es)) =>
        es should have size 1
        val ActionException(_, message, _) :: Nil = es
        message should include ("does not have an email address")
    }
  }

  it should "fail when the depositorID column is not in the dataset" in {
    val dataset = mutable.HashMap(
      "TEST_COLUMN" -> List("abc", "def")
    )
    mockLdapForDatamanager(correctDatamanagerAttrs)

    inside(AddPropertiesToDeposit(1, (datasetID, dataset)).checkPreconditions) {
      case Failure(CompositeException(es)) =>
        es should have size 1
        val ActionException(_, message, _) :: Nil = es
        message should include ("is not present")
    }
  }

  it should "fail when the depositorID column contains multiple different values" in {
    val dataset = mutable.HashMap(
      "DEPOSITOR_ID" -> List("dp1", "dp1", "dp2", "dp1")
    )
    mockLdapForDatamanager(correctDatamanagerAttrs)

    inside(AddPropertiesToDeposit(1, (datasetID, dataset)).checkPreconditions) {
      case Failure(CompositeException(es)) =>
        es should have size 1
        val ActionException(_, message, _) :: Nil = es
        message should include ("multiple distinct")
    }
  }

  it should "fail if ldap identifies the depositorID as not active" in {
    mockLdapForDepositor(false)
    mockLdapForDatamanager(correctDatamanagerAttrs)

    inside(AddPropertiesToDeposit(1, (datasetID, dataset)).checkPreconditions) {
      case Failure(CompositeException(es)) =>
        es should have size 1
        val ActionException(_, message, _) :: Nil = es
        message should include ("""depositor "dp1" is not an active user""")
    }
  }

  it should "fail if ldap does not return anything for the depositor" in {
    (ldapMock.query(_: String)(_: Attributes => Boolean)) expects ("dp1", *) returning Success(Seq.empty)
    mockLdapForDatamanager(correctDatamanagerAttrs)

    inside(AddPropertiesToDeposit(1, (datasetID, dataset)).checkPreconditions) {
      case Failure(CompositeException(es)) =>
        es should have size 1
        val ActionException(_, message, _) :: Nil = es
        message should include ("""DepositorID "dp1" is unknown""")
    }
  }

  it should "fail if ldap returns multiple values" in {
    (ldapMock.query(_: String)(_: Attributes => Boolean)) expects ("dp1", *) returning Success(Seq(true, true))
    mockLdapForDatamanager(correctDatamanagerAttrs)

    inside(AddPropertiesToDeposit(1, (datasetID, dataset)).checkPreconditions) {
      case Failure(CompositeException(es)) =>
        es should have size 1
        val ActionException(_, message, _) :: Nil = es
        message should include ("""multiple users with id "dp1"""")
    }
  }

  "execute" should "generate the properties file" in {
    mockLdapForDatamanager(correctDatamanagerAttrs)

    AddPropertiesToDeposit(1, (datasetID, dataset)).execute shouldBe a[Success[_]]

    new File(outputDepositDir(datasetID), "deposit.properties") should exist
  }

  "writeProperties" should "generate the properties file and write the properties in it" in {
    mockLdapForDatamanager(correctDatamanagerAttrs)

    AddPropertiesToDeposit(1, (datasetID, dataset)).execute shouldBe a[Success[_]]

    val props = outputPropertiesFile(datasetID)
    val content = props.read()
    content should include ("state.label")
    content should include ("state.description")

    content should include ("depositor.userId=dp1")

    content should include ("datamanager.email=dm@test.org")
    content should include ("datamanager.userId=dm")
  }
}
