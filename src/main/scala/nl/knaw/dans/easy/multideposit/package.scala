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
package nl.knaw.dans.easy

import java.io.{ File, IOException }
import java.nio.charset.Charset
import java.util.Properties

import nl.knaw.dans.easy.multideposit.model.DepositId
import nl.knaw.dans.lib.error._
import org.apache.commons.io.{ Charsets, FileExistsException, FileUtils }
import org.apache.commons.lang.StringUtils

import scala.collection.JavaConversions.collectionAsScalaIterable
import scala.util.{ Failure, Success, Try }
import scala.xml.{ Elem, PrettyPrinter, XML }

package object multideposit {

  type Datamanager = String
  type DatamanagerEmailaddress = String

  case class DepositPermissions(permissions: String, group: String)
  case class Settings(multidepositDir: File = null,
                      stagingDir: File = null,
                      outputDepositDir: File = null,
                      datamanager: Datamanager = null,
                      depositPermissions: DepositPermissions = null,
                      private val formatsFile: File = null,
                      ldap: Ldap = null) {
    val formats: Set[String] = Option(formatsFile).fold(Set.empty[String])(_.read().lines.toSet)

    override def toString: String =
      s"Settings(multideposit-dir=$multidepositDir, " +
        s"staging-dir=$stagingDir, " +
        s"output-deposit-dir=$outputDepositDir" +
        s"datamanager=$datamanager, " +
        s"deposit-permissions=$depositPermissions, " +
        s"formats=${ formats.mkString("{", ", ", "}") })"
  }

  case class EmptyInstructionsFileException(file: File) extends Exception(s"The given instructions file in '$file' is empty")
  case class ParserFailedException(report: String, cause: Throwable = null) extends Exception(report, cause)
  case class PreconditionsFailedException(report: String, cause: Throwable = null) extends Exception(report, cause)
  case class ActionRunFailedException(report: String, cause: Throwable = null) extends Exception(report, cause)
  case class ParseException(row: Int, message: String, cause: Throwable = null) extends Exception(message, cause)
  case class ActionException(row: Int, message: String, cause: Throwable = null) extends Exception(message, cause)

  object Version {
    def apply(): String = {
      val properties = new Properties()
      properties.load(getClass.getResourceAsStream("/Version.properties"))
      properties.getProperty("process-sip.version")
    }
  }

  implicit class StringExtensions(val s: String) extends AnyVal {
    /**
     * Checks whether the `String` is blank
     * (according to org.apache.commons.lang.StringUtils.isBlank)
     *
     * @return
     */
    def isBlank: Boolean = StringUtils.isBlank(s)

    /**
     * Converts a `String` to an `Option[String]`. If the `String` is blank
     * (according to org.apache.commons.lang.StringUtils.isBlank)
     * the empty `Option` is returned, otherwise the `String` is returned
     * wrapped in an `Option`.
     *
     * @return an `Option` of the input string that indicates whether it is blank
     */
    def toOption: Option[String] = {
      if (s.isBlank) Option.empty
      else Option(s)
    }

    /**
     * Converts a `String` into an `Option[Int]` if it is not blank
     * (according to org.apache.commons.lang.StringUtils.isBlank).
     * Strings that do not represent a number will yield an empty `Option`.
     *
     * @return an `Option` of the input string, converted as a number if it is not blank
     */
    def toIntOption: Option[Int] = {
      Try {
        if (s.isBlank) Option.empty
        else Option(s.toInt)
      } getOrElse Option.empty
    }
  }

  implicit class TryExceptionHandling[T](val t: Try[T]) extends AnyVal {
    /**
     * Terminating operator for `Try` that converts the `Failure` case in a value.
     *
     * @param handle converts `Throwable` to a value of type `T`
     * @return either the value inside `Try` (on success) or the result of `handle` (on failure)
     */
    def onError[S >: T](handle: Throwable => S): S = {
      t match {
        case Success(value) => value
        case Failure(throwable) => handle(throwable)
      }
    }

    def ifSuccess(f: T => Unit): Try[T] = {
      t match {
        case success @ Success(x) => Try {
          f(x)
          return success
        }
        case e => e
      }
    }

    def ifFailure(f: PartialFunction[Throwable, Unit]): Try[T] = {
      t match {
        case failure @ Failure(e) if f.isDefinedAt(e) => Try {
          f(e)
          return failure
        }
        case x => x
      }
    }

    def combine[S, R](other: Try[S])(implicit ev: T <:< (S => R)): Try[R] = {
      (t, other) match {
        case (Success(f), Success(s)) => Try { f(s) }
        case (Success(_), Failure(e)) => Failure(e)
        case (Failure(e), Success(_)) => Failure(e)
        case (Failure(CompositeException(es1)), Failure(CompositeException(es2))) => Failure(CompositeException(es1 ++ es2))
        case (Failure(CompositeException(es1)), Failure(e2)) => Failure(CompositeException(es1 ++ List(e2)))
        case (Failure(e1), Failure(CompositeException(es2))) => Failure(CompositeException(List(e1) ++ es2))
        case (Failure((e1)), Failure((e2))) => Failure(CompositeException(List(e1, e2)))
      }
    }
  }

  implicit class FileExtensions(val file: File) extends AnyVal {
    /**
     * Writes a CharSequence to a file creating the file if it does not exist using the default encoding for the VM.
     *
     * @param data the content to write to the file
     */
    @throws[IOException]("in case of an I/O error")
    def write(data: String, encoding: Charset = encoding): Unit = FileUtils.write(file, data, encoding)

    /**
     * Writes the xml to `file` and prepends a simple xml header: `<?xml version="1.0" encoding="UTF-8"?>`
     *
     * @param elem     the xml to be written
     * @param encoding the encoding applied to this xml
     */
    @throws[IOException]("in case of an I/O error")
    def writeXml(elem: Elem, encoding: Charset = encoding): Unit = {
      file.getParentFile.mkdirs()
      XML.save(file.toString, elem, encoding.toString, xmlDecl = true)
    }

    /**
     * Appends a CharSequence to a file creating the file if it does not exist using the default encoding for the VM.
     *
     * @param data the content to write to the file
     */
    @throws[IOException]("in case of an I/O error")
    def append(data: String): Unit = FileUtils.write(file, data, true)

    /**
     * Reads the contents of a file into a String using the default encoding for the VM.
     * The file is always closed.
     *
     * @return the file contents, never ``null``
     */
    @throws[IOException]("in case of an I/O error")
    def read(encoding: Charset = encoding): String = FileUtils.readFileToString(file, encoding)

    /**
     * Determines whether the ``parent`` directory contains the ``child`` element (a file or directory).
     * <p>
     * Files are normalized before comparison.
     * </p>
     *
     * Edge cases:
     * <ul>
     * <li>A ``directory`` must not be null: if null, throw IllegalArgumentException</li>
     * <li>A ``directory`` must be a directory: if not a directory, throw IllegalArgumentException</li>
     * <li>A directory does not contain itself: return false</li>
     * <li>A null child file is not contained in any parent: return false</li>
     * </ul>
     *
     * @param child the file to consider as the child.
     * @return true is the candidate leaf is under by the specified composite. False otherwise.
     */
    @throws[IOException]("if an IO error occurs while checking the files.")
    def directoryContains(child: File): Boolean = FileUtils.directoryContains(file, child)

    /**
     * Copies a file to a new location preserving the file date.
     * <p>
     * This method copies the contents of the specified source file to the
     * specified destination file. The directory holding the destination file is
     * created if it does not exist. If the destination file exists, then this
     * method will overwrite it.
     * <p>
     * <strong>Note:</strong> This method tries to preserve the file's last
     * modified date/times using File#setLastModified(long), however
     * it is not guaranteed that the operation will succeed.
     * If the modification operation fails, no indication is provided.
     *
     * @param destFile  the new file, must not be ``null``
     */
    @throws[NullPointerException]("if source or destination is null")
    @throws[IOException]("if source or destination is invalid")
    @throws[IOException]("if an IO error occurs during copying")
    def copyFile(destFile: File): Unit = FileUtils.copyFile(file, destFile)

    /**
     * Copies a whole directory to a new location preserving the file dates.
     * <p>
     * This method copies the specified directory and all its child
     * directories and files to the specified destination.
     * The destination is the new location and name of the directory.
     * <p>
     * The destination directory is created if it does not exist.
     * If the destination directory did exist, then this method merges
     * the source with the destination, with the source taking precedence.
     * <p>
     * <strong>Note:</strong> This method tries to preserve the files' last
     * modified date/times using ``File#setLastModified(long)``, however
     * it is not guaranteed that those operations will succeed.
     * If the modification operation fails, no indication is provided.
     *
     * @param destDir the new directory, must not be ``null``
     */
    @throws[NullPointerException]("if source or destination is null")
    @throws[IOException]("if source or destination is invalid")
    @throws[IOException]("if an IO error occurs during copying")
    def copyDir(destDir: File): Unit = FileUtils.copyDirectory(file, destDir)

    /**
     * Moves a directory.
     * <p>
     * When the destination directory is on another file system, do a "copy and delete".
     *
     * @param destDir the destination directory
     */
    @throws[NullPointerException]("if source or destination is null")
    @throws[FileExistsException]("if the destination directory exists")
    @throws[IOException]("if source or destination is invalid")
    @throws[IOException]("if an IO error occurs moving the file")
    def moveDir(destDir: File): Unit = FileUtils.moveDirectory(file, destDir)

    /**
     * Deletes a directory recursively.
     */
    @throws[IOException]("in case deletion is unsuccessful")
    def deleteDirectory(): Unit = FileUtils.deleteDirectory(file)

    /**
     * Finds files within a given directory and its subdirectories.
     *
     * @return a ``List`` of ``java.io.File`` with the files
     */
    def listRecursively: List[File] = FileUtils.listFiles(file, null, true).toList
  }

  val encoding = Charsets.UTF_8
  val bagDirName = "bag"
  val dataDirName = "data"
  val metadataDirName = "metadata"
  val instructionsFileName = "instructions.csv"
  val datasetMetadataFileName = "dataset.xml"
  val fileMetadataFileName = "files.xml"
  val propsFileName = "deposit.properties"

  private def datasetDir(depositId: DepositId)(implicit settings: Settings): String = {
    s"${ settings.multidepositDir.getName }-$depositId"
  }

  def multiDepositInstructionsFile(baseDir: File): File = {
    new File(baseDir, instructionsFileName)
  }

  // mdDir/depositId/
  def multiDepositDir(depositId: DepositId)(implicit settings: Settings): File = {
    new File(settings.multidepositDir, depositId)
  }

  // mdDir/instructions.csv
  def multiDepositInstructionsFile(implicit settings: Settings): File = {
    multiDepositInstructionsFile(settings.multidepositDir)
  }

  // stagingDir/mdDir-depositId/
  def stagingDir(depositId: DepositId)(implicit settings: Settings): File = {
    new File(settings.stagingDir, datasetDir(depositId))
  }

  // stagingDir/mdDir-depositId/bag/
  def stagingBagDir(depositId: DepositId)(implicit settings: Settings): File = {
    new File(stagingDir(depositId), bagDirName)
  }

  // stagingDir/mdDir-depositId/bag/data/
  def stagingBagDataDir(depositId: DepositId)(implicit settings: Settings): File = {
    new File(stagingBagDir(depositId), dataDirName)
  }

  // stagingDir/mdDir-depositId/bag/metadata/
  def stagingBagMetadataDir(depositId: DepositId)(implicit settings: Settings): File = {
    new File(stagingBagDir(depositId), metadataDirName)
  }

  // stagingDir/mdDir-depositId/deposit.properties
  def stagingPropertiesFile(depositId: DepositId)(implicit settings: Settings): File = {
    new File(stagingDir(depositId), propsFileName)
  }

  // stagingDir/mdDir-depositId/bag/metadata/dataset.xml
  def stagingDatasetMetadataFile(depositId: DepositId)(implicit settings: Settings): File = {
    new File(stagingBagMetadataDir(depositId), datasetMetadataFileName)
  }

  // stagingDir/mdDir-depositId/bag/metadata/files.xml
  def stagingFileMetadataFile(depositId: DepositId)(implicit settings: Settings): File = {
    new File(stagingBagMetadataDir(depositId), fileMetadataFileName)
  }

  // outputDepositDir/mdDir-depositId/
  def outputDepositDir(depositId: DepositId)(implicit settings: Settings): File = {
    new File(settings.outputDepositDir, datasetDir(depositId))
  }
}
