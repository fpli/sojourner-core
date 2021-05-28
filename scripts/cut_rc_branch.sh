#!/usr/bin/env bash

if [ "$#" -ne 2 ]; then
  echo "Usage: $0 <BASE_BRANCH> <VERSION>"
  exit 1
fi

BRANCH=$1
RC_BRANCH_VERSION=$2

working_dir=$( cd "$(dirname "${BASH_SOURCE[0]}")" ; pwd -P )
cd ${working_dir}/..

CURRENT_VERSION=$(cat pomVersion)

git checkout $BRANCH

if git diff-index --quiet HEAD --; then
  # No changes
  if git checkout -b release/${RC_BRANCH_VERSION}; then
    # Change pom
    if [[ "${CURRENT_VERSION}" =~ .*"-SNAPSHOT"$ ]]; then
      NEW_VERSION=${CURRENT_VERSION//SNAPSHOT/RC}
      mvn versions:set -DnewVersion=${NEW_VERSION}
      echo ${NEW_VERSION} > pomVersion
      git add .
      git commit -m "change pom version to ${NEW_VERSION}"
      # git push -u origin release/${RC_BRANCH}

      git checkout $BRANCH
    else
      echo "Not based on snapshot"
      exit 1
    fi
  else
    exit 1
  fi
else
    echo "You have uncommitted changes, please commit first!"
    exit 1
fi