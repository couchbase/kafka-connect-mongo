#!/bin/bash -e

MVN_VERSION=3.9.2

usage() {
    echo "Usage: $0 -p PRODUCT -r RELEASE -v VERSION -b BLD_NUM"
    exit 1
}

while getopts ":p:r:v:b:h?" opt; do
    case $opt in
        p) PRODUCT=$OPTARG ;;
        r) RELEASE=$OPTARG ;;
        v) VERSION=$OPTARG ;;
        b) BLD_NUM=$OPTARG ;;
        h|?) usage ;;
        :) echo "-${OPTARG} requires an argument"
           usage
           ;;
    esac
done

if [ -z "${PRODUCT}" -o -z "${RELEASE}" -o \
     -z "${VERSION}" -o -z "${BLD_NUM}" ]; then
    usage
fi

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
cd "${SCRIPT_DIR}"

if ! type mvn >& /dev/null; then
    TOOLS_DIR="$(pwd)/../tools"
    mkdir -p "${TOOLS_DIR}"
    cbdep install -d "${TOOLS_DIR}" mvn ${MVN_VERSION}
    export PATH=${TOOLS_DIR}/mvn-${MVN_VERSION}/bin:${PATH}
fi

PRODUCT_VERSION="${VERSION}.${BLD_NUM}"
mvn versions:set -DnewVersion="${PRODUCT_VERSION}"
mvn clean verify -Dquick

# Place desired output jars into dist/ directory at root of repo sync.
# Update this script if the set of desired jars change.
DIST_DIR="$(pwd)/../dist"
mkdir -p "${DIST_DIR}"
cp \
    debezium-connector-mongodb/target/debezium-connector-mongodb-${PRODUCT_VERSION}-jar-with-dependencies.jar \
    "${DIST_DIR}"

cp \
    debezium-connector-mysql/target/debezium-connector-mysql-${PRODUCT_VERSION}-jar-with-dependencies.jar \
    "${DIST_DIR}"
