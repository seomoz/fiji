#!/usr/bin/env bash

# (c) Copyright 2012 WibiData, Inc.
#
# See the NOTICE file distributed with this work for additional
# information regarding copyright ownership.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# ------------------------------------------------------------------------------

# The disable-profiling script performs clean up so that you can use the
# standard (non-profiling) jars after you are done profiling.
#
# The sequence of commands is as follows:
#
# $KIJI_HOME/bin/profiling/enable-profiling.sh
# fiji <command> etc...
# $KIJI_HOME/bin/profiling/disable-profiling.sh

# ------------------------------------------------------------------------------

set -o nounset   # Fail when referencing undefined variables
set -o errexit   # Script exits on the first error
set -o pipefail  # Pipeline status failure if any command fails
if [[ ! -z "${DEBUG:-}" ]]; then
  source=$(basename "${BASH_SOURCE}")
  PS4="# ${source}":'${LINENO}: '
  set -x
fi

# ------------------------------------------------------------------------------

if [[ -z "${KIJI_HOME}" ]]; then
  echo "Please set the KIJI_HOME enviroment variable before you enable profiling."
  exit 1
fi

if [[ ! -f "${KIJI_HOME}/conf/fiji-schema.version" ]]; then
  error "Invalid KIJI_HOME=${KIJI_HOME}"
  error "Cannot find \${KIJI_HOME}/conf/fiji-schema.version"
  exit 1
fi
fiji_schema_version=$(cat "${KIJI_HOME}/conf/fiji-schema.version")

if [[ ! -f "${KIJI_HOME}/conf/fiji-mapreduce.version" ]]; then
  error "Invalid KIJI_HOME=${KIJI_HOME}"
  error "Cannot find \${KIJI_HOME}/conf/fiji-mapreduce.version"
  exit 1
fi
fiji_mr_version=$(cat "${KIJI_HOME}/conf/fiji-mapreduce.version")

if [[ -z "${HADOOP_HOME}" ]]; then
  echo "Please set the HADOOP_HOME environment variable before you enable profiling."
  exit 1
fi

# Name of the profiling version of the fiji schema jar
fiji_profiling_schema_jar_name="fiji-schema-profiling-${fiji_schema_version}.jar"

# Name of the original fiji schema jar
fiji_schema_jar_name="fiji-schema-${fiji_schema_version}.jar"

# Name of the profiling version of the fiji mapreduce jar
fiji_profiling_mr_jar_name="fiji-mapreduce-profiling-${fiji_mr_version}.jar"

# Name of the original fiji mapreduce jar
fiji_mr_jar_name="fiji-mapreduce-${fiji_mr_version}.jar"

# The location to store the original FijiSchema and FijiMR jars during profiling
# so that they may be restored later.
orig_dir="${KIJI_HOME}/lib/original_jars"

# Flag to indicate if something unexpected was found and disabling profiling
# requires any manual intervention.
inconsistent_state="false"

# Name of aspectj jar
aspectj_jar_name="aspectjrt-1.7.2.jar"

# Remove the profiling jars from lib and distrodir. We have cp'd them while
# enabling profiling, so rm should be fine.
if [[ -f "${KIJI_HOME}/lib/${fiji_profiling_schema_jar_name}" ]]; then
  echo "Removing profile enabled fiji schema jar..."
  rm -f "${KIJI_HOME}/lib/${fiji_profiling_schema_jar_name}"
else
  echo "Did not find ${fiji_profiling_schema_jar_name} in ${KIJI_HOME}/lib. "
  echo "Is profiling enabled?"
  inconsistent_state="true"
fi

# Remove the aspectj jar
if [[ -f "${KIJI_HOME}/lib/${aspectj_jar_name}" ]]; then
  echo "Removing aspectj jar..."
  rm -f "${KIJI_HOME}/lib/${aspectj_jar_name}"
else
  echo "Did not find ${aspectj_jar_name} in ${KIJI_HOME}/lib. "
  echo "Is profiling enabled?"
  inconsistent_state="true"
fi

# Remove the FijiMR profiling-enabled jar
if [[ -f "${KIJI_HOME}/lib/${fiji_profiling_mr_jar_name}" ]]; then
  echo "Removing profile enabled fiji mapreduce jar..."
  rm -f "${KIJI_HOME}/lib/${fiji_profiling_mr_jar_name}"
else
  echo "Did not find ${fiji_profiling_mr_jar_name} in ${KIJI_HOME}/lib. "
  echo "Is profiling enabled?"
  inconsistent_state="true"
fi

# Check if the orig_dir exists and move the schema and mapreduce jars into their
# rightful places
if [[ -d "${orig_dir}" ]]; then
  if [[ ! -f "${orig_dir}/${fiji_schema_jar_name}" ]]; then
    echo "Cannot find original schema jar in ${orig_dir}. " \
      "Please move the jar ${fiji_schema_jar_name} to ${KIJI_HOME}/lib"
    inconsistent_state="true"
  else
    echo "Moving ${orig_dir}/${fiji_schema_jar_name} to ${KIJI_HOME}/lib/ ..."
    mv "${orig_dir}/${fiji_schema_jar_name}" "${KIJI_HOME}/lib/"
  fi

  if [[ ! -f "${orig_dir}/${fiji_mr_jar_name}" ]]; then
    echo "Cannot find original mapreduce jar in ${orig_dir}. " \
      "Please move the jar ${fiji_mr_jar_name} to ${KIJI_HOME}/lib"
    inconsistent_state="true"
  else
    echo "Moving ${orig_dir}/${fiji_mr_jar_name} to ${KIJI_HOME}/lib/ ..."
    mv "${orig_dir}/${fiji_mr_jar_name}" "${KIJI_HOME}/lib/"
  fi
else
  echo "Did not find ${orig_dir}. This may be because profiling was not enabled."
  echo "Ensure that ${KIJI_HOME}/lib/ has the " \
    "${fiji_schema_jar_name} and ${fiji_mr_jar_name} files."
  inconsistent_state="true"
fi

# Remove the directory which was holding the normal jars
if [[ -d "${orig_dir}" ]]; then
  rmdir "${orig_dir}"
else
  echo "Directory ${orig_dir} not found."
  inconsistent_state="true"
fi

if ! "${inconsistent_state}"; then
  echo ""
  echo "Profiling jars have been disabled. " \
    "The normal Fiji modules have been restored."
  echo ""
else
  echo "Please check the error messages. " \
    "Some manual actions may be required to disable profiling."
  exit 1
fi
