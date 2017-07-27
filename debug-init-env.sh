#!/usr/bin/env bash
#
# Copyright (C) 2016 DANS - Data Archiving and Networked Services (info@dans.knaw.nl)
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

DATADIR=data

touch ${DATADIR}/easy-split-multi-deposit.log

echo "Copy test input into $DATADIR..."
mkdir ${DATADIR}/input
cp -r src/test/resources/allfields/input ${DATADIR}/input/allfields
cp -r src/test/resources/invalidCSV/input ${DATADIR}/input/invalidCSV

echo "Creating staging and output directories in $DATADIR..."
mkdir ${DATADIR}/staging
mkdir ${DATADIR}/output
chmod -R 777 ${DATADIR}

echo "OK"
