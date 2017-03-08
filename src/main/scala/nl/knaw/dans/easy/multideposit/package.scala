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
package nl.knaw.dans.easy

import java.io.{ File, IOException }
import java.nio.charset.Charset
import java.util.Properties

import org.apache.commons.io.{ Charsets, FileExistsException, FileUtils }
import org.apache.commons.lang.StringUtils

import scala.collection.JavaConversions.collectionAsScalaIterable
import scala.collection.immutable.IndexedSeq
import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.util.{ Failure, Success, Try }
import scala.xml.{ Elem, PrettyPrinter }

package object multideposit {

  type DatasetID = String
  type MultiDepositKey = String
  type MultiDepositValues = List[String]
  type Dataset = mutable.HashMap[MultiDepositKey, MultiDepositValues]
  type Datasets = ListBuffer[(DatasetID, Dataset)]
  def Datasets: Datasets = ListBuffer.empty

  type DatasetRow = mutable.HashMap[MultiDepositKey, String]

  case class FileParameters(row: Option[Int], sip: Option[String], dataset: Option[String],
                            storageService: Option[String], storagePath: Option[String],
                            audioVideo: Option[String])
  case class DepositPermissions(permissions: String, group: String)
  case class Settings(multidepositDir: File = null,
                      springfieldInbox: File = null,
                      stagingDir: File = null,
                      outputDepositDir: File = null,
                      datamanager: String = null,
                      depositPermissions: DepositPermissions = null,
                      ldap: Ldap = null) {
    override def toString: String =
      s"Settings(multideposit-dir=$multidepositDir, " +
        s"springfield-inbox=$springfieldInbox, " +
        s"staging-dir=$stagingDir, " +
        s"output-deposit-dir=$outputDepositDir" +
        s"datamanager=$datamanager, " +
        s"deposit-permissions=$depositPermissions)"
  }

  case class EmptyInstructionsFileException(file: File) extends Exception(s"The given instructions file in '$file' is empty")
  case class PreconditionsFailedException(report: String, cause: Throwable = null) extends Exception(report, cause)
  case class ActionRunFailedException(report: String, cause: Throwable = null) extends Exception(report, cause)
  case class ActionException(row: Int, message: String, cause: Throwable = null) extends RuntimeException(message, cause)

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
    def toOption: Option[String] = if (s.isBlank) Option.empty else Option(s)

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
        case success@Success(x) => Try {
          f(x)
          return success
        }
        case e => e
      }
    }

    def ifFailure(f: PartialFunction[Throwable, Unit]): Try[T] = {
      t match {
        case failure@Failure(e) if f.isDefinedAt(e) => Try {
          f(e)
          return failure
        }
        case x => x
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
     * Writes the xml to a `File` and prepends a simple xml header: `<?xml version="1.0" encoding="UTF-8"?>`
     *
     * @param elem the xml to be written
     * @param encoding the encoding applied to this xml
     */
    @throws[IOException]("in case of an I/O error")
    def writeXml(elem: Elem, encoding: Charset = encoding): Unit = {
      val header = s"""<?xml version="1.0" encoding="$encoding"?>\n"""
      val data = new PrettyPrinter(160, 2).format(elem)

      file.write(header + data, encoding)
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
     * modified date/times using File.setLastModified(long), however
     * it is not guaranteed that those operations will succeed.
     * If the modification operation fails, no indication is provided.
     *
     * @param destDir the new directory, must not be ``null``
     */
    @throws[NullPointerException]("if source or destination is null")
    @throws[IOException]("if source or destination is invalid")
    @throws[IOException]("if an IO error occurs during copying")
    def copyFile(destDir: File): Unit = FileUtils.copyFile(file, destDir)

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

  implicit class DatasetExtensions(val dataset: Dataset) extends AnyVal {
    /**
     * Retrieves the value of a certain parameter from the dataset on a certain row.
     * If either the key is not present, the specified row does not exist or the value `blank`
     * (according to org.apache.commons.lang.StringUtils.isBlank), then Option.empty
     * is returned.
     *
     * @param key the key under which the value is stored in the dataset
     * @param row the row on which the value occurs
     * @return the value belonging to the (key, row) pair if present, else Option.empty
     */
    def getValue(key: MultiDepositKey)(row: Int): Option[String] = {
      for {
        values <- dataset.get(key)
        value <- Try(values(row)).toOption
        value2 <- value.toOption
      } yield value2
    }

    /**
     * Turns a map of key-column pairs into a filtered sequence of maps:
     * a map of key-value pairs per row, only those rows with a value for at least one of the desired columns.
     *
     * @param desiredColumns the keys of these key-value pairs specify the desired column keys
     * (the values specifying the DDM equivalent are ignored)
     * @return A sequence of maps, each map containing key-value pairs of a row.
     * Values are neither null nor blank, rows are not empty.
     * The keyset of each map is a non-empty subset of the keyset of dictionary.
     */
    def rowsWithValuesFor(desiredColumns: DDM.Dictionary): IndexedSeq[DatasetRow] =
      dataset.getColumnsIn(desiredColumns).toRows.filter(_.nonEmpty)

    def rowsWithValuesFor(desiredColumns: String*): IndexedSeq[DatasetRow] =
      dataset.getColumns(desiredColumns: _*).toRows.filter(_.nonEmpty)

    /**
     * Turns a map of key-column pairs into a filtered sequence of maps:
     * a map of key-value pairs per row and, those rows with a value for each desired column.
     * Rows with values for some but not all desired columns are ignored.
     *
     * @param desiredColumns the keys of these key-value pairs specify the desired column keys
     * (the values specifying the DDM equivalent are ignored)
     * @return A sequence of maps, each map containing key-value pairs of a row.
     * Values are neither null nor blank, rows are not empty,
     * The keyset of each map equals the keyset of dictionary.
     */
    def rowsWithValuesForAllOf(desiredColumns: DDM.Dictionary): IndexedSeq[DatasetRow] =
      dataset.getColumnsIn(desiredColumns).toRows.filter(_.size == desiredColumns.size)

    /**
     * Filters a map of key-column pairs.
     *
     * @param desiredColumns the keys of these key-value pairs specify the desired column keys
     * (the values specifying the DDM equivalent are ignored)
     * @return A map with those key-column pairs for which the key is in keyset of the dictionary.
     */
    def getColumnsIn(desiredColumns: DDM.Dictionary): Dataset =
      dataset.filter(kvs => desiredColumns.contains(kvs._1))

    def getColumns(columns: String*): Dataset =
      dataset.filter(kvs => columns.contains(kvs._1))

    /**
     * Turns a map of key-column pairs into a sequence of maps: one map of key-value pairs per row.
     *
     * @return A sequence of maps, each map containing key-value pairs of a row.
     * Values are neither null nor blank, a row may be empty.
     */
    def toRows: IndexedSeq[DatasetRow] =
      dataset.values.headOption
        .map(_.indices
          .map(i => dataset.map { case (key, values) => (key, values(i)) })
          .map(_.filter(kv => kv._2 != null && !kv._2.isBlank)))
        .getOrElse(IndexedSeq())
  }

  val encoding = Charsets.UTF_8
  val bagDirName = "bag"
  val dataDirName = "data"
  val metadataDirName = "metadata"
  val instructionsFileName = "instructions.csv"
  val datasetMetadataFileName = "dataset.xml"
  val fileMetadataFileName = "files.xml"
  val propsFileName = "deposit.properties"
  val springfieldActionsFileName = "springfield-actions.xml"

  private def datasetDir(datasetID: DatasetID)(implicit settings: Settings): String = {
    s"${settings.multidepositDir.getName}-$datasetID"
  }
  def multiDepositInstructionsFile(baseDir: File): File = {
    new File(baseDir, instructionsFileName)
  }

  // mdDir/datasetID/
  def multiDepositDir(datasetID: DatasetID)(implicit settings: Settings): File = {
    new File(settings.multidepositDir, datasetID)
  }
  // mdDir/instructions.csv
  def multiDepositInstructionsFile(implicit settings: Settings): File = {
    multiDepositInstructionsFile(settings.multidepositDir)
  }
  // stagingDir/mdDir-datasetID/
  def stagingDir(datasetID: DatasetID)(implicit settings: Settings): File = {
    new File(settings.stagingDir, datasetDir(datasetID))
  }
  // stagingDir/mdDir-datasetID/bag/
  def stagingBagDir(datasetID: DatasetID)(implicit settings: Settings): File = {
    new File(stagingDir(datasetID), bagDirName)
  }
  // stagingDir/mdDir-datasetID/bag/data/
  def stagingBagDataDir(datasetID: DatasetID)(implicit settings: Settings): File = {
    new File(stagingBagDir(datasetID), dataDirName)
  }
  // stagingDir/mdDir-datasetID/bag/metadata/
  def stagingBagMetadataDir(datasetID: DatasetID)(implicit settings: Settings): File = {
    new File(stagingBagDir(datasetID), metadataDirName)
  }
  // stagingDir/mdDir-datasetID/deposit.properties
  def stagingPropertiesFile(datasetID: DatasetID)(implicit settings: Settings): File = {
    new File(stagingDir(datasetID), propsFileName)
  }
  // stagingDir/mdDir-datasetID/bag/metadata/dataset.xml
  def stagingDatasetMetadataFile(datasetID: DatasetID)(implicit settings: Settings): File = {
    new File(stagingBagMetadataDir(datasetID), datasetMetadataFileName)
  }
  // stagingDir/mdDir-datasetID/bag/metadata/files.xml
  def stagingFileMetadataFile(datasetID: DatasetID)(implicit settings: Settings): File = {
    new File(stagingBagMetadataDir(datasetID), fileMetadataFileName)
  }
  // sfiDir/<fileMd>
  def springfieldInboxDir(fileMd: String)(implicit settings: Settings): File = {
    new File(settings.springfieldInbox, fileMd)
  }
  // sfiDir/springfield-actions.xml
  def springfieldInboxActionsFile(implicit settings: Settings): File = {
    springfieldInboxDir(springfieldActionsFileName)
  }
  // outputDepositDir/mdDir-datasetID/
  def outputDepositDir(datasetID: DatasetID)(implicit settings: Settings): File = {
    new File(settings.outputDepositDir, datasetDir(datasetID))
  }
}
