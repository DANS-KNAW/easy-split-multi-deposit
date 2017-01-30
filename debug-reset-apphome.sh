#!/usr/bin/env bash

APPHOME=home
TEMPDIR=data

rm -fr $APPHOME
cp -r src/main/assembly/dist $APPHOME
cp src/test/resources/debug-config/* $APPHOME/cfg/

if [ -e $TEMPDIR ]; then
    mv $TEMPDIR $TEMPDIR-`date  +"%Y-%m-%d@%H:%M:%S"`
fi

mkdir -p $TEMPDIR
mkdir $TEMPDIR/output/
mkdir $TEMPDIR/springfield-inbox
chmod -R 777 $TEMPDIR

echo "A fresh application home directory for debugging has been set up at $APPHOME"
echo "Output will go to $TEMPDIR/output"
echo "Add the following VM options to your run configuration to use these directories during debugging:"
echo "-Dapp.home=$APPHOME -Dlogback.configurationFile=$APPHOME/cfg/logback.xml"
