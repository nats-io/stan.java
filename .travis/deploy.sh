#!/bin/sh

echo "executing deploy.sh"

if [ ! -z "$TRAVIS_TAG" ]
then
    echo "on a tag -> set pom.xml <version> to $TRAVIS_TAG"
    mvn --settings .travis/settings.xml versions:set -DnewVersion=$TRAVIS_TAG 1>/dev/null 2>/dev/null
else
    echo "not on a tag -> keep snapshot version in pom.xml"
fi

# mvn deploy --settings .travis/settings.xml -Dskip.unit.tests=true -DperformRelease=true -B

# ${TRAVIS_BUILD_DIR}/.travis/publish-javadoc.sh
