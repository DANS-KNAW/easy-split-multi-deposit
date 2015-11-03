package nl.knaw.dans.easy.ps

import scala.xml.{Elem, PrettyPrinter}
import rx.lang.scala.Observable

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

  val allFields = "ROW" :: "DATASET_ID" :: List(profileFields, metadataFields, composedCreatorFields, composedContributorFields).map(_.keySet).flatten

  def isPartOfProfile(key: MdKey)             = profileFields.keySet.contains(key)
  def isPartOfMetadata(key: MdKey)            = metadataFields.keySet.contains(key)
  def isPartOfComposedCreator(key: MdKey)     = composedCreatorFields.keySet.contains(key)
  def isPartOfComposedContributor(key: MdKey) = composedContributorFields.keySet.contains(key)

  def datasetsToXml(datasets: Datasets): Observable[String] =
    Observable.from(datasets.map(_._2)).map(DDM.datasetToXml).map(_.toString)

  def datasetToXml(dataset: Dataset) = {
    new PrettyPrinter(160, 2).format(
    <ddm:DDM
      xmlns:ddm="http://easy.dans.knaw.nl/schemas/md/ddm/"
      xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
      xmlns:dc="http://purl.org/dc/elements/1.1/"
      xmlns:dct="http://purl.org/dc/terms/"
      xmlns:dcterms="http://purl.org/dc/terms/"
      xmlns:dcmitype="http://purl.org/dc/dcmitype/"
      xmlns:dcx="http://easy.dans.knaw.nl/schemas/dcx/"
      xmlns:dcx-dai="http://easy.dans.knaw.nl/schemas/dcx/dai/"
      xmlns:dcx-gml="http://easy.dans.knaw.nl/schemas/dcx/gml/"
      xmlns:narcis="http://easy.dans.knaw.nl/schemas/vocab/narcis-type/"
      xmlns:abr="http://www.den.nl/standaard/166/Archeologisch-Basisregister/"
      xsi:schemaLocation="http://easy.dans.knaw.nl/schemas/md/ddm/ http://easy.dans.knaw.nl/schemas/md/2012/11/ddm.xsd">
      { createProfile(dataset) }
      { createMetadata(dataset) }
    </ddm:DDM>)
  }

  def createProfile(dataset: Dataset) = {
    <ddm:profile>
      { profileElems(dataset, "DC_TITLE") }
      { profileElems(dataset, "DC_DESCRIPTION") }
      { createComposedCreators(dataset) }
      { profileElems(dataset, "DDM_CREATED") }
      { profileElems(dataset, "DDM_AUDIENCE") }
      { profileElems(dataset, "DDM_ACCESSRIGHTS") }
    </ddm:profile>
  }
  
  def profileElems(dataset: Dataset, key: MdKey) = {
    elemsFromKeyValues(key, dataset.getOrElse(key, List()))
  }

  def elemsFromKeyValues(key: MdKey, values: MdValues) = {
    values.filter(_.nonEmpty).map(value =>
      <key>{value}</key>.copy(label=profileFields.getOrElse(key, key)))
  }

  def createComposedCreators(dataset: Dataset) =
    createComposedAuthors(dataset, isPartOfComposedCreator, createComposedCreator)

  def createComposedContributors(dataset: Dataset) =
    createComposedAuthors(dataset, isPartOfComposedContributor, createComposedContributor)

  def createComposedAuthors(dataset: Dataset, isPartOfAuthor: (MdKey => Boolean), createAuthor: (Dictionary, Iterable[(MdKey, String)]) => Elem ) = {
    val authorsData = dataset.filter(x => isPartOfAuthor(x._1))
    if(authorsData.nonEmpty)
      (0 to authorsData.values.head.size-1)
        .map(i => authorsData.map { case (key, values) => (key, values(i)) })
        .filter(author => author.values.exists(x => x != null && x.length > 0))
        .map(creator => createComposedCreator(composedCreatorFields, creator))
  }

  def createComposedCreator(dictionary: Dictionary, authorFields: Iterable[(MdKey, String)]) = {
    <dcx-dai:creatorDetails>
      {
      if (isOrganization(authorFields))
        <dcx-dai:organization>
          <dcx-dai:name xml:lang="en">
            {authorFields.find(field => isOrganizationKey(field._1)).getOrElse(("",""))._2}
          </dcx-dai:name>
        </dcx-dai:organization>
      else
        <dcx-dai:author>
          {authorFields.map(composedEntry(dictionary))}
        </dcx-dai:author>
      }
    </dcx-dai:creatorDetails>
  }

  def isOrganization(authorFields: Iterable[(MdKey, String)]): Boolean = {
    val othersEmpty = authorFields
      .filterNot(field => isOrganizationKey(field._1))
      .forall(field => field._2 == "")
    val hasOrganization = authorFields.toList.exists(field => isOrganizationKey(field._1))
    othersEmpty && hasOrganization
  }

  def isOrganizationKey(key: MdKey) = key match {
    case "DCX_CREATOR_ORGANIZATION" => true
    case "DCX_CONTRIBUTOR_ORGANIZATION" => true
    case _ => false
  }

  def createComposedContributor(dictionary: Dictionary, authorFields: Iterable[(MdKey, String)]) = {
    <dcx-dai:contributorDetails>
      <dcx-dai:author>
        { authorFields.map(composedEntry(dictionary)) }
      </dcx-dai:author>
    </dcx-dai:contributorDetails>
  }

  def composedEntry (dictionary: Dictionary) (entry: (MdKey, String)) = {
    if (entry._1.endsWith("_ORGANIZATION")) {
      <dcx-dai:organization>
        <dcx-dai:name xml:lang="en">{ entry._2 }</dcx-dai:name>
      </dcx-dai:organization>
    } else {
      <key>{ entry._2 }</key>.copy(label=dictionary.getOrElse(entry._1, entry._1))
    }
  }

  def createMetadata(dataset: Dataset) = {
    <ddm:dcmiMetadata>
      { dataset.filter(kv => isPartOfMetadata(kv._1) && kv._2.nonEmpty)
               .flatMap {case (key, values) => simpleMetadataEntryToXML(key, values) } }
      { createComposedContributors(dataset) }
    </ddm:dcmiMetadata>
  }

  def simpleMetadataEntryToXML(key: MdKey, values: MdValues) = {
    values.filter(_.nonEmpty).map(value =>
      <key>{ value }</key>.copy(label=metadataFields.getOrElse(key, key)))
  }

}
