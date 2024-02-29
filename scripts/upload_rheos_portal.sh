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

# project root as working dir
pushd "$(dirname "${BASH_SOURCE[0]}")/.." >/dev/null || exit

if [[ -d "${MODULE}" && -e "${MODULE}/pom.xml" ]]; then
  pushd "${MODULE}" >/dev/null || exit
  # find artifact jar file in target folder
  JAR_FILE_CNT=$(find target -name "*.jar" -not -name "original*" 2>/dev/null | wc -l)
  if [[ $JAR_FILE_CNT -eq 1 ]]; then

    if [[ "$(cat ../pomVersion)" == *-SNAPSHOT ]]; then
      # snapshot version, need to generate unique build number as part of jar version
      echo "pom version is snapshot, need to generate unique build number as part of jar version"

      JAR_FILE_NAME=$(find target -name "*.jar" -not -name "original*" -print0)

      # jar file contains snapshot keyword
      if [[ $JAR_FILE_NAME =~ SNAPSHOT.jar ]]; then
        # detect env
        if [[ -z $BUILD_NUMBER ]]; then
          # local env, use nt and build time as BUILD_NUM
          echo "local env, use nt and build time as BUILD_NUM"
          BUILD_NUM=$(whoami).$(find target -name "*.jar" -not -name "original*" -print0 | xargs -I _ date -r _ +"%Y%m%d.%H%M%S")
        else
          # CI env, use Jenkins BUILD_NUMBER env as BUILD_NUM
          echo "CI env, use Jenkins BUILD_NUMBER env as BUILD_NUM"
          BUILD_NUM=$BUILD_NUMBER
        fi

        # rename jar file with generated build number
        mv "$JAR_FILE_NAME" "${JAR_FILE_NAME//-SNAPSHOT/.$BUILD_NUM}"

      else
        # already renamed the snapshot jar with generated build number
        echo "already renamed the snapshot jar with generated build number, no need to regenerate build number"
        BUILD_NUM=$(echo "$JAR_FILE_NAME" | grep -o -E '\w+\.[0-9]{8}\.[0-9]{6}')
      fi

    fi

    JAR_NAME=$(mvn help:evaluate -Dexpression=project.artifactId -q -DforceStdout)
    JAR_TAG=$(sed "s/-SNAPSHOT/.${BUILD_NUM}/" <../pomVersion)

    echo "${JAR_NAME}"
    echo "${JAR_TAG}"

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
