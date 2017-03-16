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
package nl.knaw.dans.easy.multideposit

import nl.knaw.dans.easy.multideposit.DDM._

object Headers {
  val springfieldHeaders = List("SF_DOMAIN", "SF_USER", "SF_COLLECTION", "SF_ACCESSIBILITY")
  val audioVideoHeaders = List("AV_FILE", "AV_FILE_TITLE", "AV_SUBTITLES", "AV_SUBTITLES_LANGUAGE")
  val administrativeHeaders = List("DEPOSITOR_ID")

  val validHeaders: List[DatasetID] = {
    val ddmHeaders = List(
      profileFields,
      metadataFields,
      composedCreatorFields,
      composedContributorFields,
      composedSpatialPointFields,
      composedSpatialBoxFields,
      composedRelationFields,
      composedTemporalFields,
      composedSubjectFields)
      .flatMap(_.keySet)

    "ROW" ::
      "DATASET" ::
      springfieldHeaders ++
        audioVideoHeaders ++
        administrativeHeaders ++
        ddmHeaders
  }
}
