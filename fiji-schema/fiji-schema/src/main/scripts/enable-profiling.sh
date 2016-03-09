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

# The enable-profiling script performs the actions required to use the
# profiling jars for FijiSchema and FijiMR, instead of standard jars.
# The corresponding disable-profiling script performs clean up so that
# you can use the standard (non-profiling) jars again.
#
# Run the command as follows before running any fiji commands
#
# $FIJI_HOME/bin/profiling/enable-profiling.sh
# fiji <command> etc...
# $FIJI_HOME/bin/profiling/disable-profiling.sh

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

if [[ -z "${FIJI_HOME}" ]]; then
  echo "Please set the FIJI_HOME enviroment variable before you enable profiling."
  exit 1
fi

if [[ ! -f "${FIJI_HOME}/conf/fiji-schema.version" ]]; then
  error "Invalid FIJI_HOME=${FIJI_HOME}"
  error "Cannot find \${FIJI_HOME}/conf/fiji-schema.version"
  exit 1
fi
fiji_schema_version=$(cat "${FIJI_HOME}/conf/fiji-schema.version")

if [[ ! -f "${FIJI_HOME}/conf/fiji-mapreduce.version" ]]; then
  error "Invalid FIJI_HOME=${FIJI_HOME}"
  error "Cannot find \${FIJI_HOME}/conf/fiji-mapreduce.version"
  exit 1
fi
fiji_mr_version=$(cat "${FIJI_HOME}/conf/fiji-mapreduce.version")

if [[ -z "${HADOOP_HOME}" ]]; then
  echo "Please set the HADOOP_HOME environment variable before you enable profiling."
  exit 1
fi

# Name of the profiled version of the FijiSchema jar
profiling_fiji_schema_jar_name="fiji-schema-profiling-${fiji_schema_version}.jar"
# Name of the original FijiSchema jar
fiji_schema_jar_name="fiji-schema-${fiji_schema_version}.jar"

# Name of the profiled version of the FijiSchema jar
profiling_fiji_mr_jar_name="fiji-mapreduce-profiling-${fiji_mr_version}.jar"
# Name of the original FijiSchema jar
fiji_mr_jar_name="fiji-mapreduce-${fiji_mr_version}.jar"

# Create a directory to move the non-profiling jars to, so we can install
# profiling-enabled jars in their normal place
orig_dir="${FIJI_HOME}/lib/original_jars"

# Name of the aspectj jar
aspectj_jar_name="aspectjrt-1.7.2.jar"

# Create a directory for original jars
mkdir -p "${orig_dir}"

# Flag to indicate some unexpected things happened along the way. We may be
# in an inconsistent state
inconsistent_state="false"

# Move FijiSchema jar out of the way
if [[ -f "${FIJI_HOME}/lib/${fiji_schema_jar_name}" ]]; then
  echo "Moving the non-profiled FijiSchema jar from "\
  "${FIJI_HOME}/lib/${fiji_schema_jar_name} to ${orig_dir}"
  mv "${FIJI_HOME}/lib/${fiji_schema_jar_name}" "${orig_dir}"
else
  echo "${FIJI_HOME}/lib/${fiji_schema_jar_name} does not exist. Is profiling enabled?"
  inconsistent_state="true"
fi

# Copy profiling FijiSchema jar into the $FIJI_HOME/lib directory
if [[ ! -f "${FIJI_HOME}/lib/${profiling_fiji_schema_jar_name}" ]]; then
  echo "Moving the profiling enabled FijiSchema jar from " \
  "${FIJI_HOME}/lib/profiling/${profiling_fiji_schema_jar_name} to ${FIJI_HOME}/lib"
  cp "${FIJI_HOME}/lib/profiling/${profiling_fiji_schema_jar_name}" "${FIJI_HOME}/lib"
else
  echo "Profiling enabled jar already exists in ${FIJI_HOME}/lib. Not overwriting."
  inconsistent_state="true"
fi

# Copy the aspectj jar into the $FIJI_HOME/lib directory
if [[ ! -f "${FIJI_HOME}/lib/${aspectj_jar_name}" ]]; then
  echo "Moving the aspectj jar from " \
  "${FIJI_HOME}/lib/profiling/${aspectj_jar_name} to ${FIJI_HOME}/lib"
  cp "${FIJI_HOME}/lib/profiling/${aspectj_jar_name}" "${FIJI_HOME}/lib"
else
  echo "Aspectj jar already exists in ${FIJI_HOME}/lib. Not overwriting."
  inconsistent_state="true"
fi

# Move Fiji MapReduce jar out of the way
if [[ -f "${FIJI_HOME}/lib/${fiji_mr_jar_name}" ]]; then
  echo "Moving the non-profiled Fiji MapReduce jar from "\
  "${FIJI_HOME}/lib/${fiji_mr_jar_name} to ${orig_dir}"
  mv "${FIJI_HOME}/lib/${fiji_mr_jar_name}" "${orig_dir}"
else
  echo "${FIJI_HOME}/lib/${fiji_mr_jar_name} does not exist. Is profiling enabled?"
  inconsistent_state="true"
fi

# Copy profiling Fiji MapReduce jar into the $FIJI_HOME/lib directory
if [[ ! -f "${FIJI_HOME}/lib/${profiling_fiji_mr_jar_name}" ]]; then
  echo "Moving the profiling enabled Fiji MapReduce jar from " \
  "${FIJI_HOME}/lib/profiling/${profiling_fiji_mr_jar_name} to ${FIJI_HOME}/lib"
  cp "${FIJI_HOME}/lib/profiling/${profiling_fiji_mr_jar_name}" "${FIJI_HOME}/lib"
else
  echo "Profiling enabled jar already exists in ${FIJI_HOME}/lib. Not overwriting."
  inconsistent_state="true"
fi

if ! "${inconsistent_state}"; then
  echo ""
  echo "Profiling has been enabled."
  echo ""
else
  echo "Please check the error messages. " \
    "Some manual actions may be required to enable profiling."
  exit 1
fi
