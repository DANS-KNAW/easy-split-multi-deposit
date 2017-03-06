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
import java.nio.file.{ FileSystemException, Files }
import java.nio.file.attribute.{ PosixFileAttributes, PosixFilePermission, UserPrincipalNotFoundException }

import nl.knaw.dans.easy.multideposit.{ Settings, UnitSpec, _ }
import org.scalatest.{ BeforeAndAfter, BeforeAndAfterAll }

import scala.util.{ Failure, Success }

class SetDepositPermissionsSpec extends UnitSpec with BeforeAndAfter with BeforeAndAfterAll {

  private val (user, userGroup, unrelatedGroup) = {
    import scala.sys.process._

    // don't hardcode users and groups, since we don't know what we have on travis
    val user = System.getProperty("user.name")
    val allGroups = "cut -d: -f1 /etc/group".!!.split("\n").filterNot(_ startsWith "#").toList
    val userGroups = s"id -Gn $user".!!.split(" ").toList

    println(s"user: $user")
    println(s"all groups: $allGroups")
    println(s"user groups: $userGroups")

    (user, userGroups.head, allGroups.diff(userGroups).head)
  }

  implicit val settings = Settings(
    multidepositDir = new File(testDir, "md"),
    outputDepositDir = new File(testDir, "dd"),
    depositPermissions = DepositPermissions("rwxrwx---", userGroup)
  )

  private val datasetID = "ruimtereis01"

  private val base = outputDepositDir(datasetID)
  private val folder1 = new File(base, "folder1")
  private val folder2 = new File(base, "folder2")
  private val file1 = new File(base, "file1.txt")
  private val file2 = new File(folder1, "file2.txt")
  private val file3 = new File(folder1, "file3.txt")
  private val file4 = new File(folder2, "file4.txt")
  private val filesAndFolders = List(base, folder1, folder2, file1, file2, file3, file4)

  before {
    base.mkdirs()
    folder1.mkdirs()
    folder2.mkdirs()

    file1.write("abcdef")
    file2.write("defghi")
    file3.write("ghijkl")
    file4.write("jklmno")

    for (file <- filesAndFolders) {
      file shouldBe readable
      file shouldBe writable
    }
  }

  after {
    base.deleteDirectory()
  }

  override def afterAll: Unit = testDir.getParentFile.deleteDirectory()

  "setFilePermissions" should "set the permissions of each of the files and folders to the correct permissions" in {
    assume(user != "travis")

    SetDepositPermissions(1, datasetID).execute() shouldBe a[Success[_]]

    for (file <- filesAndFolders) {
      Files.getPosixFilePermissions(file.toPath) should {
        have size 6 and contain only(
          PosixFilePermission.OWNER_READ,
          PosixFilePermission.OWNER_WRITE,
          PosixFilePermission.OWNER_EXECUTE,
          PosixFilePermission.GROUP_READ,
          PosixFilePermission.GROUP_WRITE,
          PosixFilePermission.GROUP_EXECUTE
        ) and contain noneOf(
          PosixFilePermission.OTHERS_READ,
          PosixFilePermission.OTHERS_WRITE,
          PosixFilePermission.OTHERS_EXECUTE
        )
      }

      Files.readAttributes(file.toPath, classOf[PosixFileAttributes]).group().getName shouldBe userGroup
    }
  }

  it should "fail if the group name does not exist" in {
    implicit val settings = Settings(
      multidepositDir = new File(testDir, "md"),
      outputDepositDir = new File(testDir, "dd"),
      depositPermissions = DepositPermissions("rwxrwx---", "non-existing-group-name")
    )

    inside(SetDepositPermissions(1, datasetID)(settings).execute()) {
      case Failure(ActionException(1, msg, _: UserPrincipalNotFoundException)) => msg shouldBe "Group non-existing-group-name could not be found"
    }
  }

  it should "fail if the access permissions are invalid" in {
    implicit val settings = Settings(
      multidepositDir = new File(testDir, "md"),
      outputDepositDir = new File(testDir, "dd"),
      depositPermissions = DepositPermissions("abcdefghi", "admin")
    )

    inside(SetDepositPermissions(1, datasetID)(settings).execute()) {
      case Failure(ActionException(1, msg, _: IllegalArgumentException)) => msg shouldBe "Invalid privileges (abcdefghi)"
    }
  }

  it should "fail if the user is not part of the given group" in {
    implicit val settings = Settings(
      multidepositDir = new File(testDir, "md"),
      outputDepositDir = new File(testDir, "dd"),
      depositPermissions = DepositPermissions("rwxrwx---", unrelatedGroup)
    )

    inside(SetDepositPermissions(1, datasetID)(settings).execute()) {
      case Failure(ActionException(1, msg, _: FileSystemException)) => msg should include (s"Not able to set the group to $unrelatedGroup")
    }
  }
}
