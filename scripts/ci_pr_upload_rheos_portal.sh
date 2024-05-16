#!/usr/bin/env bash

if [ "$#" -ne 1 ]; then
  echo "Usage: $0 <MODULE_NAME>"
  exit 1
fi

MODULE=$1

# _soj_svc
API_KEY=f4761fed120e468180af9f2b1ca04197
API_SECRET=VyX43liwhVRygELqVEdYsBX2OBpimBQFGa5cTEXDfxpLYRyFmLpZ8MknZZRlOBb8
NAMESPACE=sojourner-ubd

if [[ -d "${MODULE}" && -e "${MODULE}/pom.xml" ]]; then
  pushd "${MODULE}" >/dev/null || exit
  # find artifact jar file in target folder
  JAR_FILE_CNT=$(find target -name "*.jar" -not -name "original*" 2>/dev/null | wc -l)
  if [[ $JAR_FILE_CNT -eq 1 ]]; then

    JAR_FILE_NAME=$(find target -name "*.jar" -not -name "original*" -print0)

    echo "$JAR_FILE_NAME"
    echo "${ghprbPullId}"
    echo "${ghprbActualCommit}"

    SHORT_SHA1=${ghprbActualCommit:0:7}

    echo "${SHORT_SHA1}"

    # rename jar file with generated build number
    mv "$JAR_FILE_NAME" "${JAR_FILE_NAME//-SNAPSHOT/.pr.${ghprbPullId}.${SHORT_SHA1}}"

    JAR_NAME=$(../mvnw help:evaluate -Dexpression=project.artifactId -q -DforceStdout)
    JAR_TAG=$(sed "s/-SNAPSHOT/.pr.${ghprbPullId}.${SHORT_SHA1}/" <../pomVersion)

    echo "${JAR_NAME}"
    echo "${JAR_TAG}"

    echo "==================== Uploading jar to Rheos Portal ===================="
    ../mvnw job-uploader:upload \
      -Dusername=${API_KEY} \
      -Dpassword=${API_SECRET} \
      -Dnamespace=${NAMESPACE} \
      -DjobJarName=${JAR_NAME} \
      -DjobJarTag=${JAR_TAG}
  else
    echo "Cannot find any jar files in target folder"
    exit 1
  fi
else
  echo "Cannot find module ${MODULE} or pom.xml doesn't exist in the module folder"
  exit 1
fi
