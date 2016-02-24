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
package nl.knaw.dans.easy.ps

import org.apache.commons.lang.StringUtils.isBlank

@Deprecated
case class DatasetStoragePath(dataset: Dataset) {
  private val path: Option[String] = calculatedDatasetStoragePath
  
  private def calculatedDatasetStoragePath: Option[String] = {
    for {
      
      // TODO: handle cases where some of these columns do not exist
      a <- getFirstNonBlank(dataset("DCX_ORGANIZATION") ::: List("no-organization"))
      b <- getFirstNonBlank(dataset("DCX_CREATOR_DAI") ::: dataset("DCX_CREATOR_SURNAME") ::: dataset("DC_CREATOR"))
      c <- getFirstNonBlank(dataset("DC_TITLE"))
    } yield List(a, b, c).mkString("/")
  }

  private def getFirstNonBlank(list: List[String]): Option[String] =
    list.dropWhile(isBlank).headOption
  
  def get: Option[String] = path
}
