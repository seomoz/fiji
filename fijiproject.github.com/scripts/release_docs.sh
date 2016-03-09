#!/usr/bin/env bash
#
# Fiji documentation release script
# docs.fiji.org
#
# This script creates new copies of userguides and tutorials based on the development
# versions being worked on in directories named **/DEVEL.
#
# If executed as-is, nothing will change. You must update the environment variables
# at the top of this script to publish new documentation versions that correspond
# with concurrent Fiji module releases. New documentation (e.g., for the music
# recommendation tutorial) may refer to previous versions of dependencies like
# FijiSchema. If FijiSchema is not being released but the Music Recommendation
# tutorial is, you should leave the SCHEMA_VER variable alone.
#
# This script also updates _config.yml with newly-required handles to documentation
# versions.
#
# You must create links from userguides.md and apidocs/index.md to new reified
# documentation yourself.
#
# Please commit the changes to the script when you run it to generate new reified
# documentation instances from the DEVEL directories.

# Version numbers must be given in two forms:
#  * The FLAT_VERSION_NUMBER (e.g., 1_0_0_rc2)
#    used in the _config.yml file to refer to versions of modules
#
#  * The REGULAR_VER (e.g., 1.0.0-rc2)
#    that corresponds to the true version number as recorded by Maven to git
#    and our release artifacts.

# FijiSchema version
SCHEMA_FLAT_VER=1_5_0
SCHEMA_VER=1.5.0

# FijiMR version
FIJIMR_FLAT_VER=1_2_9
FIJIMR_VER=1.2.9

# FijiMR Library version
MRLIB_FLAT_VER=1_1_8
MRLIB_VER=1.1.8

# FijiExpress version
EXPRESS_FLAT_VER=2.0.4
EXPRESS_VER=2.0.4

# Scoring version
SCORING_FLAT_VER=0_16_0
SCORING_VER=0.16.0

# Music Recommendation Tutorial version
MUSIC_FLAT_VER=1_1_7
MUSIC_VER=1.1.7

# Express Music Recommendation tutorial version
EXPRESS_MUSIC_FLAT_VER=2.0.4
EXPRESS_MUSIC_VER=2.0.4

# Phonebook Tutorial version
PHONEBOOK_FLAT_VER=1_1_6
PHONEBOOK_VER=1.1.6

# REST Server version
REST_FLAT_VER=1_4_0
REST_VER=1.4.0

### Ordinary configuration does not go past this line ###

# Bash strict mode
set -o nounset   # Fail when referencing undefined variables
set -o errexit   # Script exits on the first error
set -o pipefail  # Pipeline status failure if any command fails
if [[ ! -z "${DEBUG:-}" ]]; then
  source=$(basename "${BASH_SOURCE}")
  PS4="# ${source}":'${LINENO}: ' # Prints out line numbers during the execution of the script
  set -x
fi

# Constants used later in this script
API=http://api-docs.fiji.org

bin=`dirname $0`
bin=`cd "$bin" && pwd`
top=`cd "$bin/.." && pwd`

set -e

# Change the DEVEL macros that point to the latest version under development
# to the true version numbers that are consistent at the time of release.
# For instance, FijiMR may refer to the FijiSchema userguide; this will not
# point to userguide_schema_devel, but userguide_schema_$SCHEMA_FLAT_VER instead.
#
# This same process is applied to all newly-released documentation artifacts.
fix_released_versions() {
  # This function operates on all markdown files under the current directory, recursively.
  # It is expected to be called within a specific directory to reify.

  # Reify references to module documentation.
  find . -name "*.md" -exec sed -i -e "s/api_mrlib_devel/api_mrlib_$MRLIB_FLAT_VER/g" {} \;
  find . -name "*.md" -exec sed -i -e "s/api_mr_devel/api_mr_$FIJIMR_FLAT_VER/g" {} \;
  find . -name "*.md" -exec sed -i -e "s/api_schema_devel/api_schema_$SCHEMA_FLAT_VER/g" {} \;
  find . -name "*.md" -exec sed -i -e "s/api_express_devel/api_express_$EXPRESS_FLAT_VER/g" {} \;
  find . -name "*.md" -exec sed -i -e \
      "s/userguide_mapreduce_devel/userguide_mapreduce_$FIJIMR_FLAT_VER/g" {} \;
  find . -name "*.md" -exec sed -i -e \
      "s/userguide_schema_devel/userguide_schema_$SCHEMA_FLAT_VER/g" {} \;
  find . -name "*.md" -exec sed -i -e \
      "s/userguide_rest_devel/userguide_rest_$REST_FLAT_VER/g" {} \;
  find . -name "*.md" -exec sed -i -e \
      "s/userguide_express_devel/userguide_express_$EXPRESS_FLAT_VER/g" {} \;
  find . -name "*.md" -exec sed -i -e \
      "s/tutorial_phonebook_devel/tutorial_phonebook_$PHONEBOOK_FLAT_VER/g" {} \;
  find . -name "*.md" -exec sed -i -e \
      "s/tutorial_scoring_devel/tutorial_scoring_$SCORING_FLAT_VER/g" {} \;
  find . -name "*.md" -exec sed -i -e \
      "s/userguide_scoring_devel/userguide_scoring_$SCORING_FLAT_VER/g" {} \;

  # Reify git tags that turn into code snippits and accordions.
  find . -name "*.md" -exec sed -i -e \
      's/{{site.schema_devel_branch}}/'"fiji-schema-root-$SCHEMA_VER/g" {} \;
  find . -name "*.md" -exec sed -i -e \
      's/{{site.mr_devel_branch}}/'"fiji-mapreduce-root-$FIJIMR_VER/g" {} \;
  find . -name "*.md" -exec sed -i -e \
      's/{{site.mrlib_devel_branch}}/'"fiji-mapreduce-lib-root-$MRLIB_VER/g" {} \;
  find . -name "*.md" -exec sed -i -e \
      's/{{site.music_devel_branch}}/'"fiji-music-$MUSIC_VER/g" {} \;
  find . -name "*.md" -exec sed -i -e \
      's/{{site.music_express_devel_branch}}/'"fiji-express-music-$EXPRESS_MUSIC_VER/g" {} \;
  find . -name "*.md" -exec sed -i -e \
      's/{{site.rest_devel_branch}}/'"fiji-rest-root-$REST_VER/g" {} \;
  find . -name "*.md" -exec sed -i -e \
      's/{{site.scoring_devel_branch}}/'"fiji-scoring-root-$SCORING_VER/g" {} \;

  # Update HTML links to tutorial elements
  find . -name "*.md" -exec sed -i -e \
      "s|schema/DEVEL|schema/$SCHEMA_VER|g" {} \;
  find . -name "*.md" -exec sed -i -e \
      "s|mapreduce/DEVEL|mapreduce/$FIJIMR_VER|g" {} \;
  find . -name "*.md" -exec sed -i -e \
      "s|phonebook/DEVEL|phonebook/$PHONEBOOK_VER|g" {} \;
  find . -name "*.md" -exec sed -i -e \
      "s|music-recommendation/DEVEL|music-recommendation/$MUSIC_VER|g" {} \;
  find . -name "*.md" -exec sed -i -e \
      "s|express-recommendation/DEVEL|express-recommendation/$EXPRESS_MUSIC_VER|g" {} \;
  find . -name "*.md" -exec sed -i -e \
      "s|rest/DEVEL|rest/$REST_VER|g" {} \;
  find . -name "*.md" -exec sed -i -e \
      "s|scoring/DEVEL|scoring/$SCORING_VER|g" {} \;

  # Reify release version numbers in the text.
  find . -name "*.md" -exec sed -i -e \
      's/{{site.schema_devel_version}}/'"$SCHEMA_VER/g" {} \;
  find . -name "*.md" -exec sed -i -e \
      's/{{site.mr_devel_version}}/'"$FIJIMR_VER/g" {} \;
  find . -name "*.md" -exec sed -i -e \
      's/{{site.mrlib_devel_version}}/'"$MRLIB_VER/g" {} \;
  find . -name "*.md" -exec sed -i -e \
      's/{{site.phonebook_devel_version}}/'"$PHONEBOOK_VER/g" {} \;
  find . -name "*.md" -exec sed -i -e \
      's/{{site.music_devel_version}}/'"$MUSIC_VER/g" {} \;
  find . -name "*.md" -exec sed -i -e \
      's/{{site.music_express_devel_version}}/'"$EXPRESS_MUSIC_VER/g" {} \;
  find . -name "*.md" -exec sed -i -e \
      's/{{site.express_devel_version}}/'"$EXPRESS_VER/g" {} \;
  find . -name "*.md" -exec sed -i -e \
      's/{{site.rest_devel_version}}/'"$REST_VER/g" {} \;
  find . -name "*.md" -exec sed -i -e \
      's/{{site.scoring_devel_version}}/'"$SCORING_VER/g" {} \;
}

# In turn, release each individual documentation component.
cd "$top/_posts"

if [ ! -d "userguides/schema/$SCHEMA_VER" ]; then
  # Create new FijiSchema documentation
  echo "Creating new FijiSchema user guide: $SCHEMA_VER"
  cp -a "userguides/schema/DEVEL" "userguides/schema/$SCHEMA_VER"

  pushd "userguides/schema/$SCHEMA_VER"

  # Replace devel versioning with macros that reflect the release version.
  find . -name "*.md" -exec sed -i -e "s/version: devel/version: $SCHEMA_VER/" {} \;
  find . -name "*.md" -exec sed -i -e "s/schema, devel]/schema, $SCHEMA_VER]/" {} \;

  # Replace links to development userguides and API documentation with the real latest
  # documentation artifact version macros (defined in /_config.yml).
  fix_released_versions

  # Define the new FijiSchema release in /_config.yml
  echo "userguide_schema_$SCHEMA_FLAT_VER : /userguides/schema/$SCHEMA_VER" \
      >> "$top/_config.yml"
  echo "api_schema_$SCHEMA_FLAT_VER : $API/fiji-schema/$SCHEMA_VER/org/fiji/schema" \
      >> "$top/_config.yml"

  popd
fi

if [ ! -d "userguides/mapreduce/$FIJIMR_VER" ]; then
  # Create new FijiMR documentation
  echo "Creating new FijiMR user guide: $FIJIMR_VER"
  cp -a "userguides/mapreduce/DEVEL" "userguides/mapreduce/$FIJIMR_VER"

  pushd "userguides/mapreduce/$FIJIMR_VER"

  # Replace devel versioning with macros that reflect the release version.
  find . -name "*.md" -exec sed -i -e \
      "s/version: devel/version: $FIJIMR_VER/" {} \;
  find . -name "*.md" -exec sed -i -e \
      "s/mapreduce, devel]/mapreduce, $FIJIMR_VER]/" {} \;

  fix_released_versions

  echo "api_mr_$FIJIMR_FLAT_VER : $API/fiji-mapreduce/$FIJIMR_VER/org/fiji/mapreduce" \
      >> "$top/_config.yml"
  echo "userguide_mapreduce_$FIJIMR_FLAT_VER : /userguides/mapreduce/$FIJIMR_VER" \
      >> "$top/_config.yml"

  popd
fi

if [ ! -d "userguides/rest/$REST_VER" ]; then
  # Create new REST documentation
  echo "Creating new FijiREST user guide: $REST_VER"
  cp -a "userguides/rest/DEVEL" "userguides/rest/$REST_VER"

  pushd "userguides/rest/$REST_VER"

  # Replace devel versioning with macros that reflect the release version.
  find . -name "*.md" -exec sed -i -e \
      "s/version: devel/version: $REST_VER/" {} \;
  find . -name "*.md" -exec sed -i -e \
      "s/rest, devel]/rest, $REST_VER]/" {} \;

  fix_released_versions

  echo "userguide_rest_$REST_FLAT_VER : /userguides/rest/$REST_VER" \
      >> "$top/_config.yml"

  popd
fi

if [ ! -d "userguides/express/$EXPRESS_VER" ]; then
  # Create new FijiExpress documentation
  echo "Creating new FijiExpress user guide: $EXPRESS_VER"
  cp -a "userguides/express/DEVEL" "userguides/express/$EXPRESS_VER"

  pushd "userguides/express/$EXPRESS_VER"

  # Replace devel versioning with macros that reflect the release version.
  find . -name "*.md" -exec sed -i -e "s/version: devel/version: $EXPRESS_VER/" {} \;
  find . -name "*.md" -exec sed -i -e "s/express, devel]/express, $EXPRESS_VER]/" {} \;

  # Replace links to development userguides and API documentation with the real latest
  # documentation artifact version macros (defined in /_config.yml).
  fix_released_versions

  # Define the new FijiExpress release in /_config.yml
  echo "userguide_express_$EXPRESS_FLAT_VER : /userguides/express/$EXPRESS_VER" \
      >> "$top/_config.yml"
  echo "api_express_$EXPRESS_FLAT_VER : $API/fiji-express/$EXPRESS_VER/org/fiji/express" \
      >> "$top/_config.yml"

  popd
fi

if [ ! -d "userguides/scoring/$SCORING_VER" ]; then
  # Create new FijiScoring documentation
  echo "Creating new FijiScoring user guide: $SCORING_VER"
  cp -a "userguides/scoring/DEVEL" "userguides/scoring/$SCORING_VER"

  pushd "userguides/scoring/$SCORING_VER"

  # Replace devel versioning with macros that reflect the release version.
  find . -name "*.md" -exec sed -i -e "s/version: devel/version: $SCORING_VER/" {} \;
  find . -name "*.md" -exec sed -i -e "s/scoring, devel]/scoring, $SCORING_VER]/" {} \;

  # Replace links to development userguides and API documentation with the real latest
  # documentation artifact version macros (defined in /_config.yml).
  fix_released_versions

  # Define the new FijiScoring release in /_config.yml
  echo "userguide_scoring_$SCORING_FLAT_VER : /userguides/scoring/$SCORING_VER" \
      >> "$top/_config.yml"
  echo "api_scoring_$SCORING_FLAT_VER : $API/fiji-scoring/$SCORING_VER/org/fiji/scoring" \
      >> "$top/_config.yml"

  popd
fi

if [ ! -d "tutorials/phonebook/$PHONEBOOK_VER" ]; then
  # Create a new phonebook tutorial
  echo "Creating new Phonebook tutorial: $PHONEBOOK_VER"
  cp -a "tutorials/phonebook/DEVEL" "tutorials/phonebook/$PHONEBOOK_VER"

  pushd "tutorials/phonebook/$PHONEBOOK_VER"

  find . -name "*.md" -exec sed -i -e \
      "s/phonebook-tutorial, devel]/phonebook-tutorial, $PHONEBOOK_VER]/" {} \;

  fix_released_versions

  # Add a reference to this version to the global config.
  echo "tutorial_phonebook_$PHONEBOOK_FLAT_VER : /tutorials/phonebook-tutorial/$PHONEBOOK_VER" \
      >> "$top/_config.yml"

  popd
fi

if [ ! -d "tutorials/music-recommendation/$MUSIC_VER" ]; then
  echo "Creating a new Music recommendation tutorial: $MUSIC_VER"
  cp -a "tutorials/music-recommendation/DEVEL" "tutorials/music-recommendation/$MUSIC_VER"

  pushd "tutorials/music-recommendation/$MUSIC_VER"

  # Reify this version number
  find . -name "*.md" -exec sed -i -e \
      "s/music-recommendation, devel]/music-recommendation, $MUSIC_VER]/" {} \;

  fix_released_versions

  # Add a reference to this version to the global config.
  echo "tutorial_music_$MUSIC_FLAT_VER : /tutorials/music-recommendation/$MUSIC_VER" \
      >> "$top/_config.yml"

  popd
fi

if [ ! -d "tutorials/express-recommendation/$EXPRESS_MUSIC_VER" ]; then
  echo "Creating a new Express music recommendation tutorial: $EXPRESS_MUSIC_VER"
  cp -a "tutorials/express-recommendation/DEVEL" \
      "tutorials/express-recommendation/$EXPRESS_MUSIC_VER"

  pushd "tutorials/express-recommendation/$EXPRESS_MUSIC_VER"

  # Reify this version number
  find . -name "*.md" -exec sed -i -e \
      "s/express-recommendation, devel]/express-recommendation, $EXPRESS_MUSIC_VER]/" {} \;

  fix_released_versions

  # Add a reference to this version to the global config.
  echo "tutorial_exp_music_$EXPRESS_MUSIC_FLAT_VER : /tutorials/express-recommendation/$EXPRESS_MUSIC_VER" \
      >> "$top/_config.yml"

  popd
fi

if [ ! -d "tutorials/scoring/$SCORING_VER" ]; then
  echo "Creating a new Scoring tutorial: $SCORING_VER"
  cp -a "tutorials/scoring/DEVEL" \
      "tutorials/scoring/$SCORING_VER"

  pushd "tutorials/scoring/$SCORING_VER"

  # Reify this version number
  find . -name "*.md" -exec sed -i -e "s/version: devel/version: $SCORING_VER/" {} \;
  find . -name "*.md" -exec sed -i -e \
      "s/tutorials, scoring, devel]/tutorials, scoring, $SCORING_VER]/" {} \;

  fix_released_versions

  # Add a reference to this version to the global config.
  echo "tutorial_scoring_$SCORING_FLAT_VER : /tutorials/scoring/$SCORING_VER" \
      >> "$top/_config.yml"

  popd
fi


# Check: If a new version of FijiMR lib is available than previously declared in
# _config.yml, add the api_mrlib_$MRLIB_FLAT_VER reference to the _config.yml.
set +e # It's ok to get a non-zero return value here.
grep "api_mrlib_$MRLIB_FLAT_VER :" "$top/_config.yml" >/dev/null
if [ "$?" != "0" ]; then
  # We didn't find the API reference. Add FijiMR Library API docs reference to _config.yml.
  echo "Adding FijiMR Library API docs to _config.yml: $MRLIB_VER"
  echo "api_mrlib_$MRLIB_FLAT_VER : $API/fiji-mapreduce-lib/$MRLIB_VER/org/fiji/mapreduce/lib" \
      >> "$top/_config.yml"
fi

grep "api_express_$EXPRESS_FLAT_VER :" "$top/_config.yml" >/dev/null
if [ "$?" != "0" ]; then
  # We didn't find the API reference. Add Fiji Express API docs reference to _config.yml.
  echo "Adding Fiji Express API docs to _config.yml: $EXPRESS_VER"
  echo "api_express_$EXPRESS_FLAT_VER : $API/fiji-express/$EXPRESS_VER/org/fiji/express" \
      >> "$top/_config.yml"
fi

echo ""
echo "Automated documentation release steps complete."
echo ""
echo "There's still some manual work to be done - docs release is not complete yet!"
echo ""
echo "At this point you should:"
echo " * Create new links in userguides.md, apidocs/index.md, and tutorials.md that"
echo "   point to the newly released modules."
echo " * Update the devel macros in /_config.yml to point to the next versions."
echo " * Commit these changes and push to master."
echo ""

