/**
 * Copyright (C) 2015-2016 DANS - Data Archiving and Networked Services (info@dans.knaw.nl)
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

import java.io.{File, IOException}
import java.util.Properties

import org.apache.commons.io.FileUtils
import org.apache.commons.lang.StringUtils

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.util.{Failure, Success, Try}

package object multiDeposit {

  type DatasetID = String
  type MultiDepositKey = String
  type MultiDepositValues = List[String]
  type Dataset = mutable.HashMap[MultiDepositKey, MultiDepositValues]
  type Datasets = ListBuffer[(DatasetID, Dataset)]

  case class FileParameters(row: Option[Int], sip: Option[String], dataset: Option[String],
                            storageService: Option[String], storagePath: Option[String],
                            audioVideo: Option[String])
  case class Settings(appHomeDir: File = null,
                      multidepositDir: File = null,
                      springfieldInbox: File = null,
                      depositDir: File = null) {
    override def toString: String =
      s"Settings(home=$appHomeDir, multideposit-dir=$multidepositDir, " +
        s"springfield-inbox=$springfieldInbox, " +
        s"deposit-dir=$depositDir)"
  }

  case class ActionException(row: Int, message: String) extends RuntimeException(message)

  implicit class StringExtensions(val s: String) extends AnyVal {
    /**
      * Checks whether the `String` is blank
      * (according to [[org.apache.commons.lang.StringUtils.isBlank]])
      *
      * @return
      */
    def isBlank = StringUtils.isBlank(s)

    /** Converts a `String` to an `Option[String]`. If the `String` is blank
      * (according to [[org.apache.commons.lang.StringUtils.isBlank]])
      * the empty `Option` is returned, otherwise the `String` is returned
      * wrapped in an `Option`.
      *
      * @return an `Option` of the input string that indicates whether it is blank
      */
    def toOption = if (s.isBlank) Option.empty else Option(s)

    /** Converts a `String` into an `Option[Int]` if it is not blank
      * (according to [[org.apache.commons.lang.StringUtils.isBlank]]).
      * Strings that do not represent a number will yield an empty `Option`.
      *
      * @return an `Option` of the input string, converted as a number if it is not blank
      */
    def toIntOption = {
      Try {
        if (s.isBlank) Option.empty
        else Option(s.toInt)
      } getOrElse Option.empty
    }
  }

  object Version {
    def apply(): String = {
      val properties = new Properties()
      properties.load(getClass.getResourceAsStream("/Version.properties"))
      properties.getProperty("process-sip.version")
    }
  }

  implicit class TryExceptionHandling[T](val t: Try[T]) extends AnyVal {
    /** Terminating operator for `Try` that converts the `Failure` case in a value.
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
  }

  implicit class FileExtensions(val file: File) extends AnyVal {
    /**
      * Writes a CharSequence to a file creating the file if it does not exist using the default encoding for the VM.
      *
      * @param data the content to write to the file
      * @throws IOException in case of an I/O error
      */
    def write(data: String) = FileUtils.write(file, data)

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
      * @throws IOException if an IO error occurs while checking the files.
      */
    def directoryContains(child: File) = FileUtils.directoryContains(file, child)

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
      * modified date/times using [[File#setLastModified(long)]], however
      * it is not guaranteed that those operations will succeed.
      * If the modification operation fails, no indication is provided.
      *
      * @param destDir the new directory, must not be ``null``
      * @throws NullPointerException if source or destination is ``null``
      * @throws IOException if source or destination is invalid
      * @throws IOException if an IO error occurs during copying
      */
    def copyFile(destDir: File) = FileUtils.copyFile(file, destDir)
  }

  implicit class DatasetExtensions(val dataset: Dataset) extends AnyVal {
    /**
      * Retrieves the value of a certain parameter from the dataset on a certain row.
      * If either the key is not present, the specified row does not exist or the value `blank`
      * (according to [[org.apache.commons.lang.StringUtils.isBlank]]), then [[Option.empty]]
      * is returned.
      *
      * @param key the key under which the value is stored in the dataset
      * @param row the row on which the value occurs
      * @return the value belonging to the (key, row) pair if present, else [[Option.empty]]
      */
    def getValue(key: MultiDepositKey)(row: Int) = {
      for {
        values <- dataset.get(key)
        value <- Try(values(row)).toOption
        value2 <- value.toOption
      } yield value2
    }
  }

  def depositDir(settings: Settings, datasetID: DatasetID) = {
    new File(settings.depositDir, s"${settings.multidepositDir.getName}-$datasetID")
  }
  def depositDirBag(settings: Settings, datasetID: DatasetID) = {
    new File(depositDir(settings, datasetID), "bag")
  }

  /** Extract the ''file'' parameters from a dataset and return these in a list of fileparameters.
    * The following parameters are used for this: '''ROW''', '''FILE_SIP''', '''FILE_DATASET''',
    * '''FILE_STORAGE_SERVICE''', '''FILE_STORAGE_PATH''', '''FILE_AUDIO_VIDEO'''.
    *
    * @param d the dataset from which the file parameters get extracted
    * @return the list with fileparameters values extracted from the dataset
    */
  def extractFileParametersList(d: Dataset): List[FileParameters] = {
    List("ROW", "FILE_SIP", "FILE_DATASET", "FILE_STORAGE_SERVICE", "FILE_STORAGE_PATH", "FILE_AUDIO_VIDEO")
      .map(d.get)
      .find(_.isDefined)
      .flatMap(_.map(_.size))
      .map(rowCount => (0 until rowCount)
        .map(index => {
          def valueAt(key: String): Option[String] = {
            d.get(key).flatMap(_ (index).toOption)
          }
          def intAt(key: String): Option[Int] = {
            d.get(key).flatMap(_ (index).toIntOption)
          }

          FileParameters(intAt("ROW"), valueAt("FILE_SIP"), valueAt("FILE_DATASET"),
            valueAt("FILE_STORAGE_SERVICE"), valueAt("FILE_STORAGE_PATH"),
            valueAt("FILE_AUDIO_VIDEO"))
        })
        .toList
        .filter {
          case FileParameters(_, None, None, None, None, None) => false
          case _ => true
        })
      .getOrElse(Nil)
  }

  /** Generates an error report with a `heading` and a list of `ActionException`s coming from a
    * list of `Try`s, sorted by row number. Supplying other exceptions than `ActionException` will
    * cause an `AssertionError`. `Success` input in `trys` are ignored.
    *
    * @param heading a piece of text before the list of errors
    * @param trys the failures to be reported
    * @return the error report
    */
  def generateErrorReport[T](heading: String, trys: Seq[Try[T]]): String = {
    heading.toOption.map(s => s"$s\n").getOrElse("") +
      trys.filter(_.isFailure)
        .map {
          case Failure(actionEx: ActionException) => actionEx
          // TODO add other Failure(exceptions) cases if needed and adjust the error message below accordingly
          case _ => throw new AssertionError("Only Failures of ActionException are expected here")
        }
        .sortBy(_.row)
        .map(actionEx => s" - row ${actionEx.row}: ${actionEx.message}")
        .mkString("\n")
  }
}
