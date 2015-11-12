easy-process-sip
================


SYNOPSIS
--------

    easy-process-sip [{--ebiu-dir|-e} <ebui-dir>] \
                [{--springfield-inbox|-s} <springfield-inbox>] \
                [{--username|-u} {--password|-p}] \
                <sip-dir>
                

DESCRIPTION
-----------

A command-line tool that processes the directory *sip-dir* containing a
Submission Information Package (SIP) using the instructions provided in the file
at `<sip-dir>/instructions.csv` (subsequently referred to as the [SIP Instructions]
File).

If the SIP Instructions file is found and is correct the following actions are taken:

* The SIP Instructions file is read and checked. If the contents is invalid the 
  tool reports the errors and exits.
* The necessary preconditions for the instructions to be carried out are checked. 
  If the preconditions are not met, the tool reports the problems and exits.
* One ore more Dataset Ingest Directories are prepared. The [EBIU] tool can 
  subsequently be used to ingest those directories into the EASY Fedora 
  repository.
* Some Data Files in the SIP may be sent to non-default storage locations *or*
  the prior existence of those files in these storage locations may be checked.
* Audio/Visual Data Files may be sent to a Springfield Inbox directory for
  subsequent processing by the Springfield Streaming Media Platform, along with
  a Springfield Instructions XML and---optionally---subtitles files.
  
  
ARGUMENTS
---------

--ebiu-dir | -e (optional)
:   A directory in which the Dataset Ingest Directories must be created.
    Defaults to ``$HOME/batch/ingest`` (``HOME`` is a standard environment variable that
    points to the user's home directory).

--springfield-inbox | -s (optional)
:   The inbox directory of a Springfield Streaming Media Platform installation. If not 
    specified the value of ``springfield-inbox`` in
   [``application.properties``] is used.

--username | -u and --password | -p (optional)
:   Default username and password to use for the non-default storage service. If
    not provided the program will ask the user for these credentials interactively when
    required. Use of these option is discouraged as it may lead to visibility of 
    the password in the command line history or in script files, which is a security
    risk.

sip-dir (required)
:   A directory containing the Submission Information Package (SIP)


INSTALLATION AND CONFIGURATION
------------------------------

``process-sip`` needs Java 1.6 or higher to run. To install it unzip the tar-archive
``process-sip-<version>.tgz`` to a directory of your choice (e.g., ``/opt``) and proceed
to specify the following configuration settings.


### PROCESS\_SIP\_HOME

Create the environment variable ``PROCESS_SIP_HOME`` and let it point to the top 
directory of the unzipped package (e.g., ``/opt/process-sip-<version>``). Also, add
``PROCESS_SIP_HOME/bin`` to your ``PATH`` environment variable.

### application.conf

The following settings can be configured:

springfield-inbox
:	This is the default path for the Springfield Inbox, which is used if the
    ``--springfield-inbox`` command line parameter is not specified
    
springfield-streaming-baseurl
:	The URL to use as a base for the ``STREAMING_SURROGATE_RELATION``. (See
    [here](sip-instructions.html#audio--video-springfield-metadata) for details
    
storage-services
:	A list of mappings from shortnames to URLs. The shortnames can be used in 
    the SIP Instructions file in the column FILE\_STORAGE\_SERVICE

Example:

    springfield-inbox = /mnt/springfield-inbox
    springfield-streaming-baseurl = \
     "http://streaming11.dans.knaw.nl/site/index.html?presentation="
    storage-services {
       zandbak = "http://zandbak11.dans.knaw.nl/webdav/"
       data2 = "http://data2.dans.knaw.nl/" 
    }

Note that values that contain colons (such as URLs) must be enclosed in doublequote
characters.


### logback.xml

The [logback] configuration file that determines how and how much output from
``process-sip`` is displayed. See


SIP INSTRUCTIONS FILE FORMAT
----------------------------

The SIP Instructions file format is documented [here][SIP Instructions]

[Excel to UTF-8 CSV]: https://www.ablebits.com/office-addins-blog/2014/04/24/convert-excel-csv/#export-csv-utf8
[LibreOffice]: https://www.libreoffice.org/
[Dublin Core elements]: http://www.dublincore.org/documents/dces/
[Dublin Core Term elements]: http://dublincore.org/documents/dcmi-terms/
[RFC4180]: http://tools.ietf.org/html/rfc4180
[WebDAV]: https://en.wikipedia.org/wiki/WebDAV
[logback]: http://logback.qos.ch/
[``application.yml``]: #applicationyml
[SIP Instructions]: sip-instructions.html




  







