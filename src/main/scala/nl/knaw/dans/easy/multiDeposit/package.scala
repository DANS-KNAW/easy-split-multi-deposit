package nl.knaw.dans.easy

import java.io.File
import java.util.Properties

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import org.apache.commons.lang.StringUtils._

import scala.util.{Failure, Success, Try}

package object multiDeposit {

  type DatasetID = String
  type MultiDepositKey = String
  type MultiDepositValues = List[String]
  type Dataset = mutable.HashMap[MultiDepositKey, MultiDepositValues]
  type Datasets = ListBuffer[(DatasetID, Dataset)]

  case class FileParameters(row: Option[String], sip: Option[String], dataset: Option[String],
                            storageService: Option[String], storagePath: Option[String],
                            audioVideo: Option[String])
  case class Settings(appHomeDir: File = null,
                      mdDir: File = null,
                      springfieldInbox: File = null,
                      springfieldStreamingBaseUrl: String = null) {
    override def toString: String =
      s"Settings(home=$appHomeDir, md-dir=$mdDir, springfield-inbox=$springfieldInbox, " +
        s"springfield-streaming-baseurl=$springfieldStreamingBaseUrl)"
  }

  case class ActionException(row: Int, message: String) extends RuntimeException(message)

  implicit class StringToOption(val s: String) extends AnyVal {
    /** Converts a `String` to an `Option[String]`. If the `String` is blank
      * (according to [[org.apache.commons.lang.StringUtils.isBlank]])
      * the empty `Option` is returned, otherwise the `String` is returned
      * wrapped in an `Option`.
      *
      * @return an `Option` of the input string that indicates whether it is blank
      */
    def toOption = if (isBlank(s)) Option.empty else Option(s)

    /** Converts a `String` into an `Option[Int]` if it is not blank
      * (according to [[org.apache.commons.lang.StringUtils.isBlank]]).
      * Strings that do not represent a number will yield an empty `Option`.
      *
      * @return an `Option` of the input string, converted as a number if it is not blank
      */
    def toIntOption = {
      Try {
        if (isBlank(s)) Option.empty
        else Option(s.toInt)
      } onError (_ => Option.empty)
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
    def onError(handle: Throwable => T): T = {
      t match {
        case Success(value) => value
        case Failure(throwable) => handle(throwable)
      }
    }
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

          FileParameters(valueAt("ROW"), valueAt("FILE_SIP"), valueAt("FILE_DATASET"),
            valueAt("FILE_STORAGE_SERVICE"), valueAt("FILE_STORAGE_PATH"), valueAt("FILE_AUDIO_VIDEO"))
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
    * @tparam T
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
