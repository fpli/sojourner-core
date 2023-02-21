#!/usr/bin/env bash

git status

# TODO: 1.add pre-condition check

working_dir=$( cd "$(dirname "${BASH_SOURCE[0]}")/.." ; pwd -P )
cd $working_dir

CURRENT_VERSION=$(cat pomVersion)
CURRENT_VERSION=${CURRENT_VERSION%-SNAPSHOT}
echo "Current version is: ${CURRENT_VERSION}"

sem_ver_arr=(${CURRENT_VERSION//./ })

patch=${sem_ver_arr[2]}
updated_patch=$((${patch}+1))
new_branch_version="${sem_ver_arr[0]}.${sem_ver_arr[1]}.${updated_patch}"
new_version="${new_branch_version}-SNAPSHOT"

echo "${new_version}" > .sojourner-version
echo "${new_version}" > pomVersion

git checkout -b release/"${new_branch_version}"

mvn versions:set -q -DnewVersion=${new_version} > /dev/null
echo "New version is: ${new_version}"

git add */pom.xml
git add pom.xml pomVersion
git commit -m "bump version to ${new_version} [skip-ci]"
git push --set-upstream origin release/${new_branch_version}
