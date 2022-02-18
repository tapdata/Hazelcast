#!/bin/bash

script_dir="$(
  cd $(dirname "$0")
  pwd
)"

echo "Script dir: ${script_dir}"
cd "${script_dir}"
module="all"
if [[ "${1}" != "" ]]; then
  module="${1}"
fi

echo "Deploy module: ${module}"

function deployRoot() {
  echo "Deploy hazelcast root"
  mvn clean deploy -Dquick -DskipTests -f pom-root.xml
}

function deployPersistence() {
  echo "Deploy hazelcast persistence"
  cd "hazelcast-persistence"
  mvn clean deploy -DskipTests
}

if [[ "${module}" == "root" ]]; then
  deployRoot
elif [[ "${module}" == "persistence" || "${module}" == "store" ]]; then
  deployPersistence
else
  deployRoot
  deployPersistence
fi
