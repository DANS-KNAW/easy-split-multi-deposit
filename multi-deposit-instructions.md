Multi-Deposit Instructions format
=================================

This document specifies the format of the Multi-Deposit Instructions (MDI) file. The MDI
file describes a Multi-Deposit, detailing:

* What dataset(s) to create from the Multi-Deposit
* The dataset metadata for each dataset
* Which files have audio/visual content, from which of them to derive streamable
  surrogates and where to host those.
  
General overview
----------------

* It is a UTF-8 encoded Comma Separated Values (CSV) file that conforms to
  [RFC4180]. If you are exporting an Excel spreadsheet to CSV, it is best to
  install [LibreOffice] and use its Calc application, which supports the required
  export without any extra steps. LibreOffice can also open Excel files.
* The first row of the file contains column headers.
* Subsequent rows contain values for only one target Dataset. Multiple rows may 
  together specify the values for one target Dataset. However, rows that specify
  one dataset must be grouped together.
* There are three types of values, which will be explained below:
    1. the `DATASET` column
    2. metadata element
    3. file processing instruction


1. The DATASET column
---------------------

For some Datasets there are multiple rows in the MDI file. This serves two purposes:

* some metadata fields may have more than one value 
* multiple files in the dataset may require special processing instructions

All the rows that pertain to one dataset must have the same value in the `DATASET`
column.


2. Metadata elements
--------------------

The supported metadata elements are subdivided into the following groups:

* The following [Dublin Core elements]: `DC_TITLE`, `DC	_DESCRIPTION`,
  `DC_CREATOR`*, `DC_CONTRIBUTOR`*, `DC_SUBJECT`, `DC_PUBLISHER`,
  `DC_TYPE`, `DC_FORMAT`, `DC_IDENTIFIER`, `DC_IDENTIFIER_TYPE`, `DC_SOURCE`, `DC_LANGUAGE`;
* The following [Dublin Core Term elements]: `DCT_ALTERNATIVE`, `DCT_SPATIAL`,
  `DCT_TEMPORAL`, `DCT_RIGHTSHOLDER`, `DCT_DATE`, `DCT_DATE_QUALIFIER`;
* DANS specific specializations of Dublin Core: `DCX_CREATOR_TITLES`, 
  `DCX_CREATOR_INITIALS`, `DCX_CREATOR_INSERTIONS`,
  `DCX_CREATOR_SURNAME`, `DCX_CREATOR_DAI`, `DCX_CREATOR_ORGANIZATION`, `DCX_CREATOR_ROLE`,
  `DCX_CONTRIBUTOR_TITLES`, `DCX_CONTRIBUTOR_INITIALS`,
  `DCX_CONTRIBUTOR_INSERTIONS`, `DCX_CONTRIBUTOR_SURNAME`, `DCX_CONTRIBUTOR_DAI`,
  `DCX_CONTRIBUTOR_ORGANIZATION`, `DCX_CONTRIBUTOR_ROLE`,
  `DCX_SPATIAL_SCHEME`, `DCX_SPATIAL_X`, `DCX_SPATIAL_Y`, `DCX_SPATIAL_NORTH`,
  `DCX_SPATIAL_SOUTH`, `DCX_SPATIAL_EAST`, `DCX_SPATIAL_WEST`,
  `DCT_TEMPORAL_SCHEME`, `DC_SUBJECT_SCHEME`,
  `DCX_RELATION_QUALIFIER`, `DCX_RELATION_TITLE`, `DCX_RELATION_LINK`
* Other DANS specific metadata elements: `DDM_CREATED`, `DDM_AVAILABLE`,
  `DDM_AUDIENCE`, `DDM_ACCESSRIGHTS`, `DEPOSITOR_ID`
* Fields that specify special properties for a file: `FILE_PATH`, `FILE_TITLE` and
  `FILE_ACCESSIBILITY`
* Fields that specify the relation to a streaming surrogate on the Springfield
  platform: `SF_DOMAIN`, `SF_USER`, `SF_COLLECTION`, and `SF_PLAY_MODE`
* The use of `DC_CREATOR` and `DC_CONTRIBUTOR` is deprecated in favor of the new
  `DCX_CREATOR_*` and `DCX_CONTRIBUTOR_*` fields.

The following elements are required: `DC_TITLE`, `DC_DESCRIPTION`, `DCX_CREATOR_*`
(at least both the subfields `DCX_CREATOR_INITIALS` and `DCX_CREATOR_SURNAME` *or*
the subfield `DCX_CREATOR_ORGANIZATION`), `DDM_CREATED`, `DDM_AUDIENCE`, `DDM_ACCESSRIGHTS`.

### Semantics of the columns

#### Dublin Core (Term) elements
The semantics of the [Dublin Core elements] and the [Dublin Core Term elements] are 
defined on the Dublin Core website. 

#### Creators and contributors
The `DCX_CREATOR_*` and `DCX_CONTRIBUTOR_*` elements follow the semantics of the 
corresponding Dublin Core elements. The only difference is that the description is
split into subfields that are fairly self-describing.

Note that the columns `DCX_CREATOR_ROLE` and `DCX_CONTRIBUTOR_ROLE` have to contain values from
the [DataCite ContributorType list].

#### Spatial
`DCT_SPATIAL` can contain any value that can be construed as "spatial characteristic" of the
dataset. A more specific value can be provided by means of the `DCX_SPATIAL_*` elements.

`DCX_SPATIAL_SCHEME` must currently be `RD`. Other schemes may be supported in the 
future. The scheme determines how the other `DCX_SPATIAL_*` elements are to be interpreted.
RD is the [Rijksdriehoekscoördinaten] scheme used in the Netherlands.

Either the pair `DCX_SPATIAL_X` and `DCX_SPATIAL_Y` (specifying a location) *or* all 
of `DCX_SPATIAL_NORTH`, `DCX_SPATIAL_SOUTH`, `DCX_SPATIAL_EAST`, `DCX_SPATIAL_WEST` 
(specifying a bounding box) must be used. Any other combination is illegal.

#### Relation
The generic `relation` element from Dublin Core is not supported. Only relations in the
form of URL's are accepted. `DCX_RELATION_QUALIFIER` is one of the 
[refinements of the relation element]. `DCX_RELATION_TITLE` is the title of the 
hyperlink if it is displayed on a web page and `DCX_RELATION_LINK` the URL to the
related resource. If a link is provided, a title should be given to provide context.

#### Identifier
`DC_IDENTIFIER_TYPE` gives extra meaning to the `DC_IDENTIFIER`. It can only have either one of the
following four values: {`ISBN`, `ISSN`, `NWO-PROJECTNR`, `ARCHIS-ZAAK-IDENTIFICATIE`} or be left empty.

#### Language
`DC_LANGUAGE` should be formatted as an [ISO 639-2](https://www.loc.gov/standards/iso639-2/php/code_list.php) (both `B` and `T` variants are supported).
`AV_SUBTITLE_LANGUAGE` should be formatted as an [ISO 639-1](https://www.loc.gov/standards/iso639-2/php/code_list.php).

#### Format
`DC_FORMAT` can either have free text or be one of the elements listed in the [formats list](src/main/assembly/dist/cfg/acceptedMediaTypes.txt).
In the latter case an extra `xsi:type` is added to the resulting DDM xml.

#### Type
`DC_TYPE` can only have a value from the set {`Collection`, `Dataset`, `Event`, `Image`, 
`InteractiveResource`, `MovingImage`, `PhysicalObject`, `Service`, `Software`, `Sound`, 
`StillImage`, `Text`}. If no value is given, `Dataset` is chosen as a default.

#### Date
`DCT_DATE_QUALIFIER` can only have a value from the set {`valid`, `issued`, `modified`,
`dateAccepted`, `dateCopyrighted`, `dateSubmitted`}. If one of these values is given, `DCT_DATE` has
to be a date, formatted as `yyyy-mm-dd`. If `DCT_DATE_QUALIFIER` isn't provided but the related
`DCT_DATE` is, the latter is considered to be free text.

#### File
`FILE_PATH`, `FILE_TITLE` and `FILE_ACCESSIBILITY` describe special properties of a file. For every
file that is registered here, at least `FILE_PATH` and either one of `FILE_TITLE` and `FILE_ACCESSIBILITY`
need to be provided. A file can only have one value for `FILE_TITLE` or `FILE_ACCESSIBILITY`.
`FILE_ACCESSIBILITY` provides a way to override the file-accessibility from the default
`DDM_ACCESSRIGHTS` for the specified file only.
The information found in the `FILE_*` columns is put into `files.xml` to better describe the file
at hand. The `FILE_TITLE` relation is put inside a `dcterms:title` element; the `FILE_ACCESSIBILITY`
relation is put inside the `accessibleToRights` element. Note that if no `FILE_TITLE` is described
for an audio/video file (see section on Springfield below), the file's name itself is used here.
Similar for `FILE_ACCESSIBILITY`: if it is not given for an audio/video file, the dataset's default
`DDM_ACCESSRIGHTS` is used instead.

#### Springfield
[Springfield Web TV] is the platform that DANS uses to host the streaming surrogates (versions)
of audiovisual data.

The metadata elements starting with `SF_` are used to create a Streaming Surrogate of
a audio or video presentation contained in the dataset:

* `SF_DOMAIN`, `SF_USER`, `SF_COLLECTION`, together specify a 
  presentation in Springfield that must be linked to by EASY. The link is created
  by adding a `dc:relation` metadata value to the dataset metadata. This relation
  is marked as having the `STREAMING_SURROGATE_RELATION` scheme and contains the
  URL of the Streaming Surrogate in Springfield. These fields may only be used if
  all of them are specified. Since the `SF_PRESENTATION` is not yet defined at this point in time,
  a placeholder called '_$sdo-id_' is put in its place.
* All files in a dataset that are identified as audio/video (using mimetype detection) are added
  to this presentation by identifying them as such (and providing extra metadata) in `files.xml`.
* The data provided in `SF_DOMAIN`, `SF_USER` and `SF_COLLECTION` are stored for further processing
  in the `deposit.properties` file.
* `SF_PLAY_MODE` specifies how the video's are played in Springfield. The value must either be
  `continuous` or `menu`. If no value is specified for a dataset with video/audio, the default will
  be set to `continuous`. This value is only allowed if `SF_DOMAIN`, `SF_USER` and `SF_COLLECTION`
  are provided as well.
* The columns `AV_FILE_PATH`, `AV_SUBTITLES` and `AV_SUBTITLES_LANGUAGE` are used to provide extra
  metadata specific to audio/video files. For a path to a A/V file in `AV_FILE_PATH`, the user can
  specify one or more combinations of subtitles and the corresponding language tag in `AV_SUBTITLES`
  and `AV_SUBTITLES_LANGUAGE` respectively. Note that the latter has to be formatted as an [ISO 639-1](https://www.loc.gov/standards/iso639-2/php/code_list.php).
* The information found in the `AV_*` columns is put into `files.xml` to better describe the file
  at hand. The `AV_SUBTITLE` and `AV_SUBTITLE_LANGUAGE` are declared in a `dcterms:relation` and
  its `xml:lang` attribute respectively.
* If SF_* fields are present, a `DC_FORMAT` for audio/ or video/ Internet Media Types is expected  


3. File Processing Instructions
-------------------------------

Files in the Multi-Deposit Directory are only processed if they are located in
a sub-directory that has a matching `DATASET`-value in the MDI file.

For example, let us assume that there is a Multi-Deposit at the directory
`/uploads/customer-1/multi-deposit-2016-01-01` and that the output deposits directory 
is located at `/data/csv-deposits`. Let us further suppose that the lay-out of the Multi-Deposit
Directory is as follows:

    /uploads/customer-1/deposit-2016-01-01
                             |
                             +- instructions.csv
                             |
                             +- dataset-1
                             |      |
                             |      +- subdir-x
                             |      |     |
                             |      |     +- file-y
                             |      |     |
                             |      |     +- file-z
                             |      |     |
                             |      |     +- video1.mpeg
                             |      |
                             |      +- video02.mpeg
                             |
                             +- dataset-2
                             |
                             +- videos
                                   |
                                   +- video01.mpeg
                                   |
                                   +- video02.mpeg

* The Multi-Deposit Directory is `/uploads/customer-1/deposit-2016-01-01`
* The Output Deposits Directory is `/data/csv-deposits`
* The MDI file is `/uploads/customer-1/deposit-2016-01-01/instructions.csv`

Now if the MDI file contains "dataset-1" as a value for the `DATASET` field
for one of the described datasets then the program will look and find a matching
data files directory at `/uploads/customer-1/deposit-2016-01-01/dataset-1`. The
files in this directory are considered to be the payload for the target deposit.
The relative paths in "dataset-1" will be preserved.

The resulting deposit will have the following location and lay-out:

     /data/csv-deposits/deposit-2016-01-01-dataset-1/
                                      |
                                      +- deposit.properties
                                      |
                                      +- bag
                                          |
                                          +- bagit.txt
                                          | 
                                          +- baginfo.txt
                                          |
                                          +- <manifest-files>* (multiple manifest files, not elaborated here)
                                          |
                                          +- data
                                          |    |
                                          |    +- subdir-x
                                          |    |    |
                                          |    |    +- file-y
                                          |    |    |
                                          |    |    +- file-z
                                          |    |    |
                                          |    |    +- video1.mpeg
                                          |    |
                                          |    +- video2.mpeg
                                          |
                                          +- metadata
                                               |
                                               +- dataset.xml
                                               |
                                               +- files.xml
                               
Note that to create a unique deposit-directory the Multi-Deposit Directory name is 
combined with the `DATASET` value.

[LibreOffice]: https://www.libreoffice.org/
[Dublin Core elements]: http://www.dublincore.org/documents/dces/
[Dublin Core Term elements]: http://dublincore.org/documents/dcmi-terms/
[RFC4180]: http://tools.ietf.org/html/rfc4180
[Rijksdriehoekscoördinaten]: https://nl.wikipedia.org/wiki/Rijksdriehoeksco%C3%B6rdinaten
[refinements of the relation element]: http://dublincore.org/documents/usageguide/qualifiers.shtml#isVersionOf
[Springfield Web TV]: http://noterik.github.io/
[DataCite ContributorType list]: http://schema.datacite.org/meta/kernel-4.0/include/datacite-contributorType-v4.xsd
