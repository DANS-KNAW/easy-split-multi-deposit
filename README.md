easy-split-multi-deposit
========================
[![Build Status](https://travis-ci.org/DANS-KNAW/easy-split-multi-deposit.png?branch=master)](https://travis-ci.org/DANS-KNAW/easy-split-multi-deposit)

Splits a Multi-Deposit into several deposit directories for subsequent ingest into the archive
Utility to process a Multi-Deposit prior to ingestion into the DANS EASY Archive

SYNOPSIS
--------

    easy-split-multi-deposit [{--output-deposits-dir|-d} <dir>][{--springfield-inbox|-s} <dir>] <multi-deposit-dir>


DESCRIPTION
-----------
A command line tool to process a Multi-Deposit into several Dataset Deposit. A Multi-Deposit
is a deposit containing data files and metadata for several datasets. The tool splits the
Multi-Deposit into separate Dataset Deposit directories that can be ingested into the archive by 
[easy-ingest-flow].

The Multi-Deposit may also contain audio/visual data files that are to be sent to a Springfield TV
installation to be converted to a streamable surrogate.

`easy-split-multi-deposit` is controlled by the Multi-Deposit Instructions (MDI) file. This is a CSV file,
that must be located at `<multi-deposit-dir>/instructions.csv`. It should contain the metadata for the
datasets that are to be created and may also contain instructions about how to process individual files.
See the [Multi-Deposit Instructions] page for details.

If the MDI file is found and is correct the following actions are taken:

1. The MDI file is read and checked. If the contents is invalid the tool reports the errors and exits.
2. The necessary preconditions for the instructions to be carried out are checked. 
   If the preconditions are not met, the tool reports the problems and exits.
3. One or more deposit directories are created, one for each dataset
4. Depending on whether the MDI file contains any processing instructions for the audio/visual files 
   in the Multi-Deposit they may be sent to a Springfield Inbox directory for
   subsequent processing by the Springfield Streaming Media Platform, along with
   a Springfield Actions XML and---optionally---subtitles files. 
  
  
ARGUMENTS
---------
```
  Usage: 

      easy-split-multi-deposit.sh [{--springfield-inbox|-s} <dir>] <multi-deposit-dir> <output-deposits-dir> <datamanager>

  Options:


  -s, --springfield-inbox  <arg>   The inbox directory of a Springfield Streaming Media Platform installation.
                                   If not specified the value of springfield-inbox in application.properties
                                   is used. 
      --help                       Show help message
      --version                    Show version of this program

 trailing arguments:
    multi-deposit-dir (required)   Directory containing the Submission Information Package to process. This must
                                 be a valid path to a directory containing a file named 'instructions.csv' in
                                 RFC4180 format.
    deposit-dir (required)         A directory in which the deposit directories must be created. The deposit
                                 directory layout is described in the easy-sword2 documentation
    datamanager (required)         The user id of the datamanger (archivist) performing this deposit
```


### Installation steps:

1. Unzip the tarball to a directory of your choice, e.g. `/opt/`
2. A new directory called easy-split-mult-deposit-<version> will be created, referred to below as `${app.home}`
3. Create a symbolic link to the executable script in a directory that is in your `PATH`, e.g.

        ln -s ${app.home}/bin/easy-split-multi-deposit /usr/bin/easy-split-multi-deposit


### Configuration

General configuration settings can be set in `${app.home}/cfg/application.properties` and logging can be
configured in `${app.home}/cfg/logback.xml`. The available settings are explained in comments in 
aforementioned files.


BUILDING FROM SOURCE
--------------------

Prerequisites:

* Java 8 or higher
* Maven 3.3.3 or higher
 
Steps:

        git clone https://github.com/DANS-KNAW/easy-split-multi-deposit
        cd easy-split-multi-deposit
        mvn install


[Multi-Deposit Instructions]: multi-deposit-instructions.md
[easy-ingest-flow]: https://github.com/DANS-KNAW/easy-ingest-flow

  







