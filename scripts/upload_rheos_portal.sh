#!/usr/bin/env bash

if [ "$#" -ne 1 ]; then
  echo "Usage: $0 <MODULE_NAME>"
  exit 1
fi

MODULE=$1

API_KEY=9ce5764c30224ce59bb5eb4c95afaf02
API_SECRET=K2h42hFcxWpNV33EgTluY7oekgrBmTl26u6HMWrTVnQgzaj6Vld4QGFUAiN8RUsz

if [[ -e "${MODULE}/pom.xml" ]]; then
    echo "==================== Uploading jar to Rheos Portal ===================="
    mvn -f ${MODULE}/pom.xml job-uploader:upload -Dusername=${API_KEY} -Dpassword=${API_SECRET} -Dnamespace=sojourner-ubd
else
  echo "Cannot find module ${MODULE}"
  exit 1
fi