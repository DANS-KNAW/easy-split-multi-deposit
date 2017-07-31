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

LOCAL_ARGS=$@
OUTPUT=data/output
STAGING=data/staging
RUN_SCRIPT=run.sh

ping -t 1 -c 1 test.dans.knaw.nl >/dev/null

if [[ $? == 0 ]]; then
    if [[ " ${LOCAL_ARGS[*]} " != *"--help"* ]] && [[ " ${LOCAL_ARGS[*]} " != *"--version"* ]]; then
        if [ "$(ls -A ${OUTPUT})" ]; then
            NEWOUTPUT=${OUTPUT}-$(date  +"%Y-%m-%d@%H:%M:%S")
            mv ${OUTPUT} ${NEWOUTPUT}
            mkdir ${OUTPUT}
            echo "the old output folder has been moved to $NEWOUTPUT"
        fi

        if [ "$(ls -A ${STAGING})" ]; then
            NEWSTAGING=${STAGING}-$(date  +"%Y-%m-%d@%H:%M:%S")
            mv ${STAGING} ${NEWSTAGING}
            mkdir ${STAGING}
            echo "the old staging folder has been moved to $NEWSTAGING"
        fi
    fi

    source ${RUN_SCRIPT}
else
    echo "The server at test.dans.knaw.nl is not running."
    echo "Please make sure this is running before executing this application."
fi
