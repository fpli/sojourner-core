#!/usr/bin/env bash

if [ "$#" -ne 1 ]; then
  echo "Usage: $0 <MODULE_NAME>"
  exit 1
fi

MODULE=$1

# _soj_svc
API_KEY=7fd45773637548469d728aceef7169eb
API_SECRET=0cYSzwXkFzOOweW3hLj3IETrAmVUT8ZOyf7C782fLxaZUxGo6lu7cVkSOi6pl5hD
NAMESPACE=sojourner-ubd

if [[ -d "${MODULE}" && -e "${MODULE}/pom.xml" ]]; then
  pushd "${MODULE}" >/dev/null || exit
  # find artifact jar file in target folder
  JAR_FILE_CNT=$(find target -name "*.jar" -not -name "original*" 2>/dev/null | wc -l)
  if [[ $JAR_FILE_CNT -eq 1 ]]; then

    # rename jar file with generated build number
    mv "$JAR_FILE_NAME" "${JAR_FILE_NAME//-SNAPSHOT/.pr.${ghprbPullId}.${BUILD_NUM}}"

    JAR_NAME=$(mvn help:evaluate -Dexpression=project.artifactId -q -DforceStdout)
    JAR_TAG=$(sed "s/-SNAPSHOT/.pr.${ghprbPullId}.${BUILD_NUM}/" <../pomVersion)
    #
    echo "==================== Uploading jar to Rheos Portal ===================="
    mvn job-uploader:upload \
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
