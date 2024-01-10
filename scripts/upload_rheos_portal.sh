#!/usr/bin/env bash

if [ "$#" -ne 1 ]; then
  echo "Usage: $0 <MODULE_NAME>"
  exit 1
fi

MODULE=$1

working_dir=$( cd "$(dirname "${BASH_SOURCE[0]}")" ; pwd -P )
cd ${working_dir}/..

# _soj_svc
API_KEY=7fd45773637548469d728aceef7169eb
API_SECRET=0cYSzwXkFzOOweW3hLj3IETrAmVUT8ZOyf7C782fLxaZUxGo6lu7cVkSOi6pl5hD
NAMESPACE=sojourner-ubd

if [[ -e "${MODULE}/pom.xml" ]]; then
  pushd ${MODULE}
  # find artifact in target folder
  if ls target/*.jar 1> /dev/null 2>&1; then
    # if it's in CI server env, BUILD_NUM will be set by Jenkins
    BUILD_NUM=${BUILD_NUMBER:-"$(whoami).$(date '+%Y%m%d.%H%M%S')"}
    for i in $(ls target/*.jar); do
      if [[ "${i%.jar}" == *-SNAPSHOT ]]; then
        mv "$i" "`echo $i | sed "s/-SNAPSHOT/.${BUILD_NUM}/"`";
      elif [[ "$(cat ../pomVersion)" == *-SNAPSHOT  ]]; then
        # build num already generated
        BUILD_NUM=${BUILD_NUMBER:-${i:(-19):15}}
      fi
    done
    echo "Build number is: $BUILD_NUM"

    JAR_NAME=$(mvn help:evaluate -Dexpression=project.artifactId -q -DforceStdout)
    JAR_TAG=$(cat ../pomVersion | sed "s/-SNAPSHOT/.${BUILD_NUM}/")

    echo "==================== Uploading jar to Rheos Portal ===================="
    mvn job-uploader:upload \
      -Dusername=${API_KEY} \
      -Dpassword=${API_SECRET} \
      -Dnamespace=${NAMESPACE} \
      -DjobJarName=${JAR_NAME} \
      -DjobJarTag=${JAR_TAG}
  else
    echo "Cannot find job jar file"
    exit 1
  fi
else
  echo "Cannot find module ${MODULE}"
  exit 1
fi