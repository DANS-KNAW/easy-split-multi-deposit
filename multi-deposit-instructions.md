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
  `DC_TYPE`, `DC_FORMAT`, `DC_IDENTIFIER`, `DC_SOURCE`, `DC_LANGUAGE`;
* The following [Dublin Core Term elements]: `DCT_ALTERNATIVE`, `DCT_SPATIAL`,
  `DCT_TEMPORAL`, `DCT_RIGHTSHOLDER`;
* DANS specific specializations of Dublin Core: `DCX_CREATOR_TITLES`, 
  `DCX_CREATOR_INITIALS`, `DCX_CREATOR_INSERTIONS`,
  `DCX_CREATOR_SURNAME`, `DCX_CREATOR_DAI`, `DCX_CREATOR_ORGANIZATION`,
  `DCX_CONTRIBUTOR_TITLES`, `DCX_CONTRIBUTOR_INITIALS`,
  `DCX_CONTRIBUTOR_INSERTIONS`, `DCX_CONTRIBUTOR_SURNAME`, `DCX_CONTRIBUTOR_DAI`,
  `DCX_CONTRIBUTOR_ORGANIZATION`,
  `DCX_SPATIAL_SCHEME`, `DCX_SPATIAL_X`, `DCX_SPATIAL_Y`, `DCX_SPATIAL_NORTH`,
  `DCX_SPATIAL_SOUTH`, `DCX_SPATIAL_EAST`, `DCX_SPATIAL_WEST`,
  `DCX_RELATION_QUALIFIER`, `DCX_RELATION_TITLE`, `DCX_RELATION_LINK`
* Other DANS specific metadata elements: `DDM_CREATED`, `DDM_AVAILABLE`,
  `DDM_AUDIENCE`, `DDM_ACCESSRIGHTS`
* Fields that specify the relation to a streaming surrogate on the Springfield
  platform: `SF_DOMAIN`, `SF_USER`, `SF_COLLECTION`, `SF_PRESENTATION`, 
  `SF_SUBTITLES`

\* The use of `DC_CREATOR` and `DC_CONTRIBUTOR` is deprecated in favor of the new
`DCX_CREATOR_*` and `DCX_CONTRIBUTOR_*` fields.

The following elements are required: `DC_TITLE`, `DC_DESCRIPTION`, `DCX_CREATOR_*`
(at least both the subfields `DCX_CREATOR_INITIALS` and `DCX_CREATOR_SURNAME` *or*
the subfield `DCX_CREATOR_ORGANIZATION`), `DDM_*`.

### Semantics of the columns

#### Dublin Core (Term) elements
The semantics of the [Dublin Core elements] and the [Dublin Core Term elements] are 
defined on the Dublin Core website. 

#### Creators and contributors
The `DCX_CREATOR_*` and `DCX_CONTRIBUTOR_*` elements follow the semantics of the 
corresponding Dublin Core elements. The only difference is that the description is
split into subfields that are fairly self-describing.

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
related resource.

#### Springfield
[Springfield Web TV] is the platform that DANS uses to host the streaming surrogates (versions)
of audiovisual data.

The metadata elements starting with `SF_` are used to create a Streaming Surrogate of
a audio or video presentation contained in the dataset:

* `SF_DOMAIN`, `SF_USER`, `SF_COLLECTION`, `SF_PRESENTATION`, together specify a 
  presentation in Springfield that must be linked to from EASY. The link is created
  by adding a `dc:relation` metadata value to the dataset metadata. This relation
  is marked as having the `STREAMING_SURROGATE_RELATION` scheme and contains the
  URL of said Streaming Surrogate in Springfield. These fields may only be used if
  all of them are specified.
* Files marked with `FILE_AUDIO_VIDEO` = "Yes" (see below) are added to this presentation
  through the Springfield Inbox and an entry in the Springfield Actions file that 
  will be created by `process-sip`.
* `SF_SUBTITLES` specifies a file that contains the subtitles for the presentation 
  specified.


3. File Processing Instructions
-------------------------------

### Default Processing
By default, files in the Multi-Deposit Directory are only processed if they are located in
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
                             |            |
                             |            +- file-y
                             |            |
                             |            +- file-z
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
*default* behavior now is to use the files in this directory as the payload for
the target deposit. The relative paths in "dataset-1" will be preserved.

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
                                          +- <manifest-files>* (multiple manifist files, not elaborated here)
                                          |
                                          +- data
                                          |    |
                                          |    +- subdir-x
                                          |         |
                                          |         +- file-y
                                          |         |
                                          |         +- file-z
                                          |
                                          +- metadata
                                               |
                                               +- datasets.xml
                                               |
                                               +- files.xml
                               
Note that to create a unique deposit-directory the Multi-Deposit Directory name is 
combined with the `DATASET` value.

### Non-default Processing

#### Non-default File Item Path
By default the File Item path in the Dataset will be the same as the relative path 
from the SIP-subdirectory with the matching `DATASET` name (see above). It is possible
to specify a different path for specific files. The following columns are used for this:

`FILE_SIP`
:   The relative path of the file in the SIP Directory. If this path points to a 
    directory `FILE_DATASET` will also be assumed to be indended as a directory name.
    The whole directory tree starting at `FILE_SIP` is then created in the Dataset
    at the path specified in `FILE_DATASET`.

`FILE_DATASET`
:   The relative path of the File Item in the Dataset to be created. (Or a Folder Item, 
    if `FILE_SIP` points to a directory (see previous item)).	

#### Non-default Storage
By default the data files from the SIP are staged for ingest into the default EASY
storage, which is managed by Fedora. It is possible to specify an alternative AIP-store

<!-- TO DO re-specify this -->


[LibreOffice]: https://www.libreoffice.org/
[Dublin Core elements]: http://www.dublincore.org/documents/dces/
[Dublin Core Term elements]: http://dublincore.org/documents/dcmi-terms/
[RFC4180]: http://tools.ietf.org/html/rfc4180
[Rijksdriehoekscoördinaten]: https://nl.wikipedia.org/wiki/Rijksdriehoeksco%C3%B6rdinaten
[refinements of the relation element]: http://dublincore.org/documents/usageguide/qualifiers.shtml#isVersionOf
[Springfield Web TV]: http://noterik.github.io/
