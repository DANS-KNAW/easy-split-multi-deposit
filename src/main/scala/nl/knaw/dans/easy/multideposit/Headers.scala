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
