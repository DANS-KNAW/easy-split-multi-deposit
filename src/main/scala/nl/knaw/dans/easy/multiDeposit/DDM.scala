package nl.knaw.dans.easy.multiDeposit

object DDM {
  type Dictionary = Map[String, String]

  private val profileFields : Dictionary =
    Map( "DC_TITLE"         -> "dc:title"
      , "DC_DESCRIPTION"   -> "dcterms:description"
      , "DC_CREATOR"       -> "dc:creator"
      , "DDM_CREATED"      -> "ddm:created"
      , "DDM_AUDIENCE"     -> "ddm:audience"
      , "DDM_ACCESSRIGHTS" -> "ddm:accessRights" )

  private val metadataFields : Dictionary =
    Map( "DDM_AVAILABLE"    -> "ddm:available"
      , "DC_CONTRIBUTOR"   -> "dc:contributor"
      , "DCT_ALTERNATIVE"  -> "dcterms:alternative"
      , "DC_SUBJECT"       -> "dc:subject"
      , "DC_PUBLISHER"     -> "dcterms:publisher"
      , "DC_TYPE"          -> "dcterms:type"
      , "DC_FORMAT"        -> "dc:format"
      , "DC_IDENTIFIER"    -> "dc:identifier"
      , "DC_SOURCE"        -> "dc:source"
      , "DC_LANGUAGE"      -> "dc:language"
      , "DCT_SPATIAL"      -> "dcterms:spatial"
      , "DCT_TEMPORAL"     -> "dcterms:temporal"
      , "DCT_RIGHTSHOLDER" -> "dcterms:rightsHolder")

  private val composedCreatorFields : Dictionary =
    Map( "DCX_CREATOR_TITLES"       -> "dcx-dai:titles"
      , "DCX_CREATOR_INITIALS"     -> "dcx-dai:initials"
      , "DCX_CREATOR_INSERTIONS"   -> "dcx-dai:insertions"
      , "DCX_CREATOR_SURNAME"      -> "dcx-dai:surname"
      , "DCX_CREATOR_DAI"          -> "dcx-dai:DAI"
      , "DCX_CREATOR_ORGANIZATION" -> "dcx-dai:name xml:lang=\"en\"")

  private val composedContributorFields : Dictionary =
    Map( "DCX_CONTRIBUTOR_TITLES"       -> "dcx-dai:titles"
      , "DCX_CONTRIBUTOR_INITIALS"     -> "dcx-dai:initials"
      , "DCX_CONTRIBUTOR_INSERTIONS"   -> "dcx-dai:insertions"
      , "DCX_CONTRIBUTOR_SURNAME"      -> "dcx-dai:surname"
      , "DCX_CONTRIBUTOR_DAI"          -> "dcx-dai:DAI"
      , "DCX_CONTRIBUTOR_ORGANIZATION" -> "dcx-dai:name xml:lang=\"en\"")

  val allFields = "ROW" :: "DATASET_ID" ::
    List(profileFields, metadataFields, composedCreatorFields, composedContributorFields)
      .map(_.keySet)
      .flatten
}