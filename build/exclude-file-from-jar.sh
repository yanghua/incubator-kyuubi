#!/usr/bin/env bash

#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

#
# Script to create a binary distribution for easy deploys of Kyuubi.
# The distribution directory defaults to dist/ but can be overridden below.
# The distribution contains fat (assembly) jars that include the Scala library,
# so it is completely self contained.
# It does not contain source or *.class files.

set -o pipefail
set -e
set -x

if [ "$#" -ne 2 ]; then
    echo "Usage: $0 <jar-file> <file-to-exclude>"
    exit 1
fi

JAR_FILE=$(realpath "$1")
FILE_TO_EXCLUDE=$2
TEMP_DIR=$(mktemp -d)

unzip -q "$JAR_FILE" -d "$TEMP_DIR"

rm -f "$TEMP_DIR/$FILE_TO_EXCLUDE"

cd "$TEMP_DIR"

jar cf "$JAR_FILE" *
cd -

rm -rf "$TEMP_DIR"

echo "File $FILE_TO_EXCLUDE has been excluded from $JAR_FILE"
