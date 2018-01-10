package nl.knaw.dans.easy.multideposit2.actions

import java.nio.file.attribute.{ PosixFileAttributes, PosixFilePermission, UserPrincipalNotFoundException }
import java.nio.file.{ FileSystemException, Files }

import nl.knaw.dans.easy.multideposit.FileExtensions
import nl.knaw.dans.easy.multideposit2.{ DepositPermissions, TestSupportFixture }
import org.scalatest.BeforeAndAfterEach

import scala.util.{ Failure, Properties, Success }

class SetDepositPermissionsSpec extends TestSupportFixture with BeforeAndAfterEach {

  private val (user, userGroup, unrelatedGroup) = {
    import scala.sys.process._

    // don't hardcode users and groups, since we don't know what we have on travis
    val user = Properties.userName
    val allGroups = "cut -d: -f1 /etc/group".!!.split("\n").filterNot(_ startsWith "#").toList
    val userGroups = s"id -Gn $user".!!.split(" ").toList

    (userGroups, allGroups.diff(userGroups)) match {
      case (ug :: _, diff :: _) => (user, ug, diff)
      case (Nil, _) => throw new AssertionError("no suitable user group found")
      case (_, Nil) => throw new AssertionError("no suitable unrelated group found")
    }
  }

  private val depositId = "ruimtereis01"

  private val base = stagingDir(depositId)
  private val folder1 = base.resolve("folder1")
  private val folder2 = base.resolve("folder2")
  private val file1 = base.resolve("file1.txt")
  private val file2 = folder1.resolve("file2.txt")
  private val file3 = folder1.resolve("file3.txt")
  private val file4 = folder2.resolve("file4.txt")
  private val filesAndFolders = List(base, folder1, folder2, file1, file2, file3, file4)

  override def beforeEach(): Unit = {
    Files.createDirectories(base)
    Files.createDirectories(folder1)
    Files.createDirectories(folder2)

    file1.write("abcdef")
    file2.write("defghi")
    file3.write("ghijkl")
    file4.write("jklmno")

    for (file <- filesAndFolders) {
      file.toFile shouldBe readable
      file.toFile shouldBe writable
    }
  }

  "setFilePermissions" should "set the permissions of each of the files and folders to the correct permissions" in {
    assume(user != "travis",
      "this test does not work on travis, because we don't know the group that we can use for this")

    val action = new SetDepositPermissions(DepositPermissions("rwxrwx---", userGroup))

    action.setDepositPermissions(depositId) shouldBe a[Success[_]]

    for (file <- filesAndFolders) {
      Files.getPosixFilePermissions(file) should {
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

      Files.readAttributes(file, classOf[PosixFileAttributes]).group().getName shouldBe userGroup
    }
  }

  it should "fail if the group name does not exist" in {
    val action = new SetDepositPermissions(DepositPermissions("rwxrwx---", "non-existing-group-name"))

    inside(action.setDepositPermissions(depositId)) {
      case Failure(ActionException(msg, _: UserPrincipalNotFoundException)) => msg shouldBe "Group non-existing-group-name could not be found"
    }
  }

  it should "fail if the access permissions are invalid" in {
    val action = new SetDepositPermissions(DepositPermissions("abcdefghi", "admin"))

    inside(action.setDepositPermissions(depositId)) {
      case Failure(ActionException(msg, _: IllegalArgumentException)) => msg shouldBe "Invalid privileges (abcdefghi)"
    }
  }

  it should "fail if the user is not part of the given group" in {
    val action = new SetDepositPermissions(DepositPermissions("rwxrwx---", unrelatedGroup))

    inside(action.setDepositPermissions(depositId)) {
      case Failure(ActionException(msg, _: FileSystemException)) => msg should include(s"Not able to set the group to $unrelatedGroup")
    }
  }
}