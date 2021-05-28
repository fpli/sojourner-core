#!/usr/bin/env bash

working_dir=$( cd "$(dirname "${BASH_SOURCE[0]}")" ; pwd -P )
cd ${working_dir}/..

echo "Using maven version: $(mvn -v)"
CURRENT_VERSION=$(cat pomVersion)

if [[ "${CURRENT_VERSION}" =~ .*"-SNAPSHOT"$ ]]; then
  BUILD_NUMBER=${BUILD_NUMBER:-$(date '+%Y%m%d.%H%M%S')}
  NEW_VERSION=${CURRENT_VERSION/%-SNAPSHOT/.${BUILD_NUMBER}}
else
  NEW_VERSION=${CURRENT_VERSION}
fi

echo "Build version is: ${NEW_VERSION}"
mvn versions:set -DnewVersion=${NEW_VERSION}
echo ${NEW_VERSION} > pomVersion
