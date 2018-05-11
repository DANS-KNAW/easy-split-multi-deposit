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

import java.io.IOException
import java.nio.charset.Charset
import java.nio.file.{ Files, Path }

import org.apache.commons.io.{ Charsets, FileExistsException, FileUtils }
import org.joda.time.format.{ DateTimeFormatter, ISODateTimeFormat }
import resource.managed

import scala.collection.JavaConverters._
import scala.collection.generic.CanBuildFrom
import scala.util.{ Failure, Success, Try }
import scala.xml.{ Elem, PrettyPrinter, Utility, XML }

package object multideposit {
  val dateTimeFormatter: DateTimeFormatter = ISODateTimeFormat.dateTime()

  val encoding: Charset = Charsets.UTF_8

  case class DepositPermissions(permissions: String, group: String)

  implicit class SeqExtensions[T](val seq: Seq[T]) extends AnyVal {
    def mapUntilFailure[S](f: T => Try[S])(implicit cbf: CanBuildFrom[Seq[T], S, Seq[S]]): Try[Seq[S]] = {
      val bf = cbf()
      for (t <- seq) {
        f(t) match {
          case Success(x) => bf += x
          case Failure(e) => return Failure(e)
        }
      }
      Success(bf.result())
    }
  }

  implicit class FileExtensions(val path: Path) extends AnyVal {
    /**
     * Writes a CharSequence to a file creating the file if it does not exist using the default encoding for the VM.
     *
     * @param data the content to write to the file
     */
    @throws[IOException]("in case of an I/O error")
    def write(data: String, encoding: Charset = encoding): Unit = FileUtils.write(path.toFile, data, encoding)

    /**
     * Writes the xml to `file` and prepends a simple xml header: `<?xml version="1.0" encoding="UTF-8"?>`
     *
     * @param elem     the xml to be written
     * @param encoding the encoding applied to this xml
     */
    @throws[IOException]("in case of an I/O error")
    def writeXml(elem: Elem, encoding: Charset = encoding): Unit = {
      Files.createDirectories(path.getParent)
      XML.save(path.toString, XML.loadString(new PrettyPrinter(160, 2).format(Utility.trim(elem))), encoding.toString, xmlDecl = true)
    }

    /**
     * Appends a CharSequence to a file creating the file if it does not exist using the default encoding for the VM.
     *
     * @param data the content to write to the file
     */
    @throws[IOException]("in case of an I/O error")
    def append(data: String): Unit = FileUtils.write(path.toFile, data, true)

    /**
     * Reads the contents of a file into a String using the default encoding for the VM.
     * The file is always closed.
     *
     * @return the file contents, never ``null``
     */
    @throws[IOException]("in case of an I/O error")
    def read(encoding: Charset = encoding): String = FileUtils.readFileToString(path.toFile, encoding)

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
    def directoryContains(child: Path): Boolean = FileUtils.directoryContains(path.toFile, child.toFile)

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
     * @param destFile the new file, must not be ``null``
     */
    @throws[NullPointerException]("if source or destination is null")
    @throws[IOException]("if source or destination is invalid")
    @throws[IOException]("if an IO error occurs during copying")
    def copyFile(destFile: Path): Unit = FileUtils.copyFile(path.toFile, destFile.toFile)

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
    def copyDir(destDir: Path): Unit = FileUtils.copyDirectory(path.toFile, destDir.toFile)

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
    def moveDir(destDir: Path): Unit = FileUtils.moveDirectory(path.toFile, destDir.toFile)

    /**
     * Deletes a directory recursively.
     */
    @throws[IOException]("in case deletion is unsuccessful")
    def deleteDirectory(): Unit = FileUtils.deleteDirectory(path.toFile)

    /**
     * Finds files within a given directory and its subdirectories.
     *
     * @return a ``List`` of ``java.nio.file.Path`` with the files
     */
    def listRecursively(predicate: Path => Boolean = _ => true): List[Path] = {
      managed(Files.walk(path))
        .acquireAndGet(_.iterator().asScala.filter(predicate).toList)
    }

    /**
     * Normalize the path and make it absolute. This takes '.' paths into account.
     *
     * @example
     * {{{
     *   // current path: /foo/bar
     *   Paths.get(".").normalized() // res: /foo/bar
     * }}}
     *
     * @return
     */
    def normalized(): Path = {
      val normalized = path.normalize()
      // if path == "."
      if (normalized.getFileName.toString.isEmpty)
        normalized.toAbsolutePath
      else normalized
    }
  }
}
