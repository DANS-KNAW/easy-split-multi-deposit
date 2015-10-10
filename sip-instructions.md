Introduction
============

This document specifies the format of the SIP Instructions file. The SIP Instructions
file describes a Submission Information Package, detailing:

* What dataset(s) to create from the SIP
* How to distribute the files in the over the datasets, and where to place them
  in the file hierarchy of the datasets
* Which archival storage service to use to store the files.
* Which files have audio/visual content and from which of them to derive streamable
  surrogates and where to host those.
  
General Overview
================

* It is a UTF-8 encoded Comma Separated Values (CSV) file that conforms to
  [RFC4180]. If you are exporting an Excel spreadsheet to CSV, follow the
  instructions in the blog post [Excel to UTF-8 CSV]. However, it may be easier to
  install [LibreOffice] and use its Calc application, which supports the required
  export without any extra steps. LibreOffice can also open Excel files.
* The first row of the file contains column headers.
* Subsequent rows contain values for only one target Dataset. Multiple rows may 
  together specify the values for one target Dataset.
* There are three types of values, which will be explained below:
  1. provisional dataset ID
  2. metadata value
  3. file processing instruction

Provisional Dataset ID
======================

For some Datasets there are multiple rows in the SIP Instructions file. This is
to accommodate for: 

* the fact that some metadata fields may have more than one value 
* multiple files in the dataset may require special processing instructions

All the rows that pertain to one dataset must have the same value in the ``DATASET_ID``
column. 

This ID is also used as the name of the Dataset Ingest Directory. (It is therefore
*also* possible to use ``process-sip`` to prepare Dataset Update Directories, which
the EBIU can use to update the files or metadata of an existing dataset.)


Metadata Elements
=================

The supported metadata elements are subdivided into the following groups: *TODO INDICATE
WHICH ARE REQUIRED*

* The following [Dublin Core elements]: ``DC_TITLE``, ``DC	_DESCRIPTION``,
  ``DC_CREATOR``, ``DC_CONTRIBUTOR``, ``DC_SUBJECT``, ``DC_PUBLISHER``,
  ``DC_TYPE``, ``DC_FORMAT``, ``DC_IDENTIFIER``, ``DC_SOURCE``, ``DC_LANGUAGE``
* The following [Dublin Core Term elements]: ``DCT_ALTERNATIVE``, ``DCT_SPATIAL``,
  ``DCT_TEMPORAL``, ``DCT_RIGHTSHOLDER``
* DANS specific specializations of Dublin Core: ``DCX_CREATOR_TITLES``, 
  ``DCX_CREATOR_INITIALS``, ``DCX_CREATOR_INSERTIONS``,
  ``DCX_CREATOR_SURNAME``, ``DCX_CREATOR_DAI``, ``DCX_CREATOR_ORGANIZATION``,
  ``DCX_CONTRIBUTOR_TITLES``, ``DCX_CONTRIBUTOR_INITIALS``,
  ``DCX_CONTRIBUTOR_INSERTIONS``, ``DCX_CONTRIBUTOR_SURNAME``, ``DCX_CONTRIBUTOR_DAI``,
  ``DCX_CONTRIBUTOR_ORGANIZATION``
* Other DANS specific metadata elements: ``DDM_CREATED``, ``DDM_AVAILABLE``,
  ``DDM_AUDIENCE``, ``DDM_ACCESSRIGHTS``
* Fields that specify the relation to a streaming surrogate on the Springfield
  platform: ``SF_DOMAIN``, ``SF_USER``, ``SF_COLLECTION``, ``SF_PRESENTATION``, 
  ``SF_SUBTITLES``
  
  
Audio / Video (Springfield) Metadata
------------------------------------

The metadata elements starting with ``SF_`` are used to create a Streaming Surrogate of
a audio or video presentation contained in the dataset:

* ``SF_DOMAIN``, ``SF_USER``, ``SF_COLLECTION``, ``SF_PRESENTATION``, together specify a 
  presentation in Springfield that must be linked to from EASY. The link is created
  by adding a ``dc:relation`` metadata value to the dataset metadata. This relation
  is marked as having the ``STREAMING_SURROGATE_RELATION`` scheme and contains the
  URL of said Streaming Surrogate in Springfield. These fields may only be used if
  all of them are specified.
* Files marked with ``FILE_AUDIO_VIDEO`` = "Yes" (see below) are added to this presentation
  through the Springfield Inbox and an entry in the Springfield Actions file that 
  will be created by ``process-sip``.
* ``SF_SUBTITLES`` specifies a file that contains the subtitles for the presentation 
  specified.


File Processing Instructions
============================

Default Processing
------------------

By default, files in the SIP Directory are only processed if they are located in
a sub-directory that has a matching ``DATASET_ID``-value in the SIP Instructions file.

For example, let us assume that there is a SIP at the directory
``/uploads/customer-1/sip-2015-01-01`` and that the EBIU directory is located at
``/home/jdoe/batch/ingest``. Let us further suppose that the lay-out of the SIP
Directory is as follows:

    /uploads/customer-1/sip-2015-01-01
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

If the SIP Instructions file contains rows marked (in the ``DATASET_ID`` column) with
the provisional identifiers "dataset-1" and "dataset-2" then the files in the
corresponding SIP sub-directories will *by default* be copied to
``/home/jdoe/batch/ingest/dataset-1/filedata`` and
``/home/jdoe/batch/ingest/dataset-2/filedata`` respectively. The relative path is
maintained. Specifically ``/home/jdoe/batch/ingest/dataset-1/data/subdir-x/file-y``
and ``/home/jdoe/batch/ingest/dataset-1/filedata/subdir-x/file-z`` will be created.


Non-default Processing
----------------------

### Non-default File Item Path

By default the File Item path in the Dataset will be the same as the relative path 
from the SIP-subdirectory with the matching ``DATASET_ID`` name (see above). It is possible
to specify a different path for specific files. The following columns are used for this:

FILE\_SIP
:   The relative path of the file in the SIP Directory. If this path points to a 
    directory ``FILE_DATASET`` will also be assumed to be indended as a directory name.
    The whole directory tree starting at ``FILE_SIP`` is then created in the Dataset
    at the path specified in ``FILE_DATASET``.

FILE\_DATASET
:   The relative path of the File Item in the Dataset to be created. (Or a Folder Item, 
    if ``FILE_SIP`` points to a directory (see previous item)).	

### Non-default Storage

By default the data files from the SIP are staged for ingest into the default EASY
storage, which is managed by Fedora. It is possible to specify an alternative storage
service.

###### storage service
A *storage service* is a [WebDAV]-enabled HTTP Server. The following columns
are used to specify the storage service to use and the location on the server to 
store a particular file.

FILE\_SIP
:   The relative path of the file in the SIP Directory. If this path points to a 
    directory ``FILE_DATASET`` will also be assumed to be indended as a directory name.
    The whole directory tree starting at ``FILE_SIP`` is then created the ``FILE_STORAGE_SERVICE``.

FILE\_STORAGE\_SERVICE
:   The URL of the service that provides the storage location for the file. It
    may also be a name defined in the [``application.yml``]. 
    
FILE\_STORAGE\_PATH
:   The path in the storage provided by ``FILE_STORAGE_SERVICE`` where the file is or 
    will be stored. If this value is not provided it is constructed as follows:

      CONSTRUCTED_DATASET_STORAGE_PATH/FILE_DATASET
      
    For the way CONSTRUCTED_DATASET_STORAGE_PATH is constructed see
    [below](#constructed-dataset-storage-path)   
    
FILE\_AUDIO\_VIDEO
:   "Yes" or "No" (default). If "Yes", a Streaming Surrogate of the file will be 
    prepared by the Springfield platform.   

###### actions
Depending on the presence or absence of values in these columns one or more of the 
following actions are taken:

(1) Check Existence In Storage
:   Check that a file is already present in a given storage location. Fail if it
    is not present. This is useful if a file was previouly copied manually to storage
    and you want to record metadata for it in Fedora.

(2) Copy File To Dataset Ingest Directory
:   Copy the file to Dataset Ingest Directory, so that after running [EBIU] the 
    file will be created in EASY in ``DATASET_PATH``
    
(3) Copy File To Storage
:   Copy the file to alternative archival storage location.

(4) Create Data File Instructions
:   Create a Data File Instructions file in the Dataset Ingest Directory, in the
    path indicated by ``DATASET_PATH``, so that after running [EBIU] a link to 
    a storage location is created in Fedora.
    
(5) Copy To Springfield Inbox
:   Copy the file to the Springfield Inbox with the same relative path as in the
    SIP Directory, so that Springfield derives a Streaming Surrogate from it.
    
(6) Copy Placeholder To Springfield Inbox
:   Copy a placeholder video file to the Springfield Inbox in absence of one specified
    in ``FILE_SIP``. This is useful if a Streaming Surrogate already exists and will be 
    migrated directly to the Springfield Inbox (by-passing ``process-sip``).
    
###### legal combinations
In the following the legal combinations of these values and the actions that they
trigger are detailed. All other combinations are illegal:

Type | Combination                                                       | Actions
-----|-------------------------------------------------------------------|--------------
A    | ``FILE_SIP``, ``FILE_DATASET``, (``FILE_AUDIO_VIDEO``)            | 2, (5)
-----|-------------------------------------------------------------------|--------------
B    | ``FILE_SIP``, ``FILE_DATASET``, ``FILE_STORAGE_SERVICE``,         | 2, 3, 4, (5)
     | (``FILE_AUDIO_VIDEO``)                                            | 
-----|-------------------------------------------------------------------|--------------
C    | ``FILE_SIP``, ``FILE_DATASET``, ``FILE_STORAGE_SERVICE``,         | 2, 3 ,4, (5)
     | ``FILE_STORAGE_PATH``, (``FILE_AUDIO_VIDEO``)                     | 
-----|-------------------------------------------------------------------|--------------     
D    | ``FILE_DATASET``, ``FILE_STORAGE_SERVICE``, ``FILE_STORAGE_PATH``,| 1, 4, (6)
     | (``FILE_AUDIO_VIDEO``)                                            | 

###### FILE\_AUDIO\_VIDEO always optional
``FILE_AUDIO_VIDEO`` is mentioned in parentheses to limit the list of
combinations. The action in parentheses is only added if ``FILE_AUDIO_VIDEO`` is
"Yes".

##### Constructed Dataset Storage Path
B and C differ in that in the case of B the file storage path is constructed
from metadata-values instead of being provided. This path will have the
following structure:
  
    COMPONENT1/COMPONENT2/COMPONENT3/COMPONENT4
    
in which COMPONENTX is replaced by a metadata value, normalized to ASCII and
with all spaces replaced by minus-characters. The rules for the selection of the
metadata *element* (and in one case a file processing field) to use are given
below. The elements to the left are given preference over the ones to the right.
If an element has multiple *values*, the first is used. If an element has no
value, the next to the right is tried. If for a given component, none of the
alternative elements have a value, a warning is logged and processing of the
file is skipped.
  
* COMPONENT1: DCX\_ORGANISATION, "no-organization"
* COMPONENT2: DCX\_CREATOR\_DAI, DCX\_CREATOR\_SURNAME, DC\_CREATOR
* COMPONENT3: DC\_TITLE
* COMPONENT4: FILE\_DATASET


[//]: # ----- end of document -------------------------------------------------------
[//]: # links and images
[EBIU]: {{ site.baseurl }}/reqs/ebiu
[Excel to UTF-8 CSV]: https://www.ablebits.com/office-addins-blog/2014/04/24/convert-excel-csv/#export-csv-utf8
[LibreOffice]: https://www.libreoffice.org/
[Dublin Core elements]: http://www.dublincore.org/documents/dces/
[Dublin Core Term elements]: http://dublincore.org/documents/dcmi-terms/
[RFC4180]: http://tools.ietf.org/html/rfc4180
[WebDAV]: https://en.wikipedia.org/wiki/WebDAV
[logback]: http://logback.qos.ch/
[``application.yml``]: #applicationyml

