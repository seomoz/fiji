#!/usr/bin/env bash
#
#   (c) Copyright 2013 WibiData, Inc.
#
#   See the NOTICE file distributed with this work for additional
#   information regarding copyright ownership.
#
#   Licensed under the Apache License, Version 2.0 (the "License");
#   you may not use this file except in compliance with the License.
#   You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#   Unless required by applicable law or agreed to in writing, software
#   distributed under the License is distributed on an "AS IS" BASIS,
#   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#   See the License for the specific language governing permissions and
#   limitations under the License.
#
#
#   The express script provides tools for running FijiExpress scripts and interacting with the
#   FijiExpress system.
#   Tools are run as:
#
#   bash> $EXPRESS_HOME/bin/express <tool-name> [options]
#
#   For full usage information, use:
#
#   bash> $EXPRESS_HOME/bin/express help
#

# Resolve a symlink to its absolute target, like how 'readlink -f' works on Linux.
function resolve_symlink() {
  TARGET_FILE=${1}

  if [ -z "$TARGET_FILE" ]; then
    echo ""
    return 0
  fi

  cd $(dirname "$TARGET_FILE")
  TARGET_FILE=$(basename "$TARGET_FILE")

  # Iterate down a (possible) chain of symlinks
  count=0
  while [ -L "$TARGET_FILE" ]; do
    if [ "$count" -gt 1000 ]; then
      # Just stop here, we've hit 1,000 recursive symlinks. (cycle?)
      break
    fi

    TARGET_FILE=$(readlink "$TARGET_FILE")
    cd $(dirname "$TARGET_FILE")
    TARGET_FILE=$(basename "$TARGET_FILE")
    count=$(( $count + 1 ))
  done

  # Compute the canonicalized name by finding the physical path
  # for the directory we're in and appending the target file.
  PHYS_DIR=$(pwd -P)
  RESULT="$PHYS_DIR/$TARGET_FILE"
  echo "$RESULT"
}


prgm="$0"
prgm=`resolve_symlink "$prgm"`
bin=`dirname "$prgm"`
bin=`cd "${bin}" && pwd`

echo 'WARNING: This Script is Deprecated. Support will cease in the future. Instead use:'
echo 'WARNING: bash> $EXPRESS_HOME/bin/express.py'


EXPRESS_HOME="${EXPRESS_HOME:-${bin}/../}"

# Any arguments you want to pass to FijiExpress's jvm may be done via this env var.
EXPRESS_JAVA_OPTS=${EXPRESS_JAVA_OPTS:-""}

# Any user code you want to add to the fiji classpath may be done via this env var.
# Jar files here will also be copied to the distributed cache for mapreduce jobs.
FIJI_CLASSPATH=${FIJI_CLASSPATH:-""}

# This is a workaround for OS X Lion, where a bug in JRE 1.6
# creates a lot of 'SCDynamicStore' errors.
if [ "$(uname)" == "Darwin" ]; then
  EXPRESS_JAVA_OPTS="$EXPRESS_JAVA_OPTS -Djava.security.krb5.realm= -Djava.security.krb5.kdc="
fi

# An existing set of directories to use for the java.library.path property should
# be set with JAVA_LIBRARY_PATH.
JAVA_LIBRARY_PATH=${JAVA_LIBRARY_PATH:-""}

# Try CDH defaults.
HBASE_HOME="${HBASE_HOME:-/usr/lib/hbase}"
HADOOP_HOME="${HADOOP_HOME:-/usr/lib/hadoop}"

# First make sure we have everything we need in the environment.
if [ -z "${EXPRESS_HOME}" -o ! -d "${EXPRESS_HOME}" ]; then
  echo "Please set your EXPRESS_HOME environment variable."
  exit 1
fi
if [ -z "${HBASE_HOME}" -o ! -d "${HBASE_HOME}" ]; then
  echo "Please set your HBASE_HOME environment variable."
  exit 1
fi
if [ -z "${HADOOP_HOME}" -o ! -d "${HADOOP_HOME}" ]; then
  echo "Please set your HADOOP_HOME environment variable."
  exit 1
fi

if [ -z "${1}" ]; then
  echo "express: Tool launcher for FijiExpress."
  echo "Run 'express help' to see a list of available tools."
  exit 1
fi

# Removes classpath entries that match the given regexp (partial match, not full
# match).
function remove_classpath_entries() {
  local cp=${1}
  local regex=${2}

  echo $cp | sed "s/[^:]*$regex[^:]*/::/g" | sed 's/::*/:/g'
  return 0
}

# Helper to build classpaths correctly
function append_path() {
  if [ -z "${1}" ]; then
    echo ${2}
  else
    echo ${1}:${2}
  fi
}

# Scrubs classpaths of a given jar. Mutate will dig into *s, only mutating them
# if it finds the given jar.
# mutate_classpath scrubme.jar "$(hadoop classpath)"
function mutate_classpath () {
  local mutated_classpath
  local jar_to_scrub=${1}
  shift

  # Stop expanding globs
  set -f
  IFS=: read -r -a classpath <<< ${@}

  for path in $classpath; do
    # If it ends with a glob we'll need to dig deeper for jars
    if [ "${path: -1:1}" = "*" ]; then
      set +f
      local expanded_classpath=$(JARS=(${path}.jar); IFS=:; echo "${JARS[*]}")
      set -f

      # If the expanded classpath contains the jar in question, we'll
      # scrub it later.
      if [[ $expanded_classpath =~ .*$jar_to_scrub.* ]]; then
        mutated_classpath=$(append_path $mutated_classpath $expanded_classpath)

      # If the expanded classpath doesn't contain the jar in question, use
      # the glob version to reduce clutter.
      else
        mutated_classpath=$(append_path $mutated_classpath $path)
      fi
    # No glob just use the path
    else
      mutated_classpath=$(append_path $mutated_classpath $path)
    fi
  done

  # Scrub all instances of the jar
  mutated_classpath=$(remove_classpath_entries "$mutated_classpath" "$jar_to_scrub")
  echo $mutated_classpath

  set +f
}

# Detect and extract the current Hadoop version number. e.g. "Hadoop 2.x-..." -> "2" You can
# override this with $FIJI_HADOOP_DISTRO_VER (e.g. "hadoop1" or "hadoop2").
function extract_hadoop_major_version() {
  hadoop_major_version=$(${HADOOP_HOME}/bin/hadoop version | head -1 | cut -c 8)
  if [ -z "${hadoop_major_version}" -a -z "${FIJI_HADOOP_DISTRO_VER}" ]; then
    echo "Warning: Unknown Hadoop version. May not be able to load all Fiji jars."
    echo "Set FIJI_HADOOP_DISTRO_VER to 'hadoop1' or 'hadoop2' to load these."
  else
    FIJI_HADOOP_DISTRO_VER=${FIJI_HADOOP_DISTRO_VER:-"hadoop${hadoop_major_version}"}
  fi
}

# Extracts the --libjars value from the command line and appends/prepends this value to any
# classpath variables.To ensure that --libjars doesn't get passed into the express_tool, this
# function effectively rebuilds the remainder of the command line arguments by removing the
# --libjars argument and value. The result is stored in a new global variable called ${COMMAND_ARGS}
# which is used by other parts of the script in lieu of ${@}.
function extract_classpath() {
  return_args=""
  while (( "${#}" )); do
    if [ "${1}" == "--libjars" ]; then
      libjars_cp=${2}
      shift
    else
      return_args="${return_args} ${1}"
    fi
    shift
  done
  COMMAND_ARGS=${return_args}

  # Append FIJI_CLASSPATH to libjars.
  libjars_cp="${libjars_cp}:${FIJI_CLASSPATH}"

  # Gather the express dependency jars.
  if [ -z "${FIJI_HOME}" -o ! -d "${FIJI_HOME}" ]; then
   echo "Please set your FIJI_HOME environment variable."
   exit 1
  fi
  if [ -z "${FIJI_MR_HOME}" -o ! -d "${FIJI_MR_HOME}" ]; then
   echo "Please set your FIJI_MR_HOME environment variable."
   exit 1
  fi

  # Add FijiExpress specific jars.
  express_libjars="${EXPRESS_HOME}/lib/*"

  # If SCHEMA_SHELL_HOME is set, add fiji-schema-shell jars to the classpath to enable features
  # that require it.
  schema_shell_libjars=""
  if [[ -n "${SCHEMA_SHELL_HOME}" ]]; then
    schema_shell_libjars="${SCHEMA_SHELL_HOME}/lib/*"
  fi
  # We may have Hadoop distribution-specific jars to load in
  # $FIJI_HOME/lib/distribution/hadoopN, where N is the major digit of the Hadoop
  # version. Only load at most one such set of jars.
  extract_hadoop_major_version

  # If MODELING_HOME is set, add fiji-modeling jars to the classpath to enable using the express
  # script with fiji-modeling.  THIS IS A HACK!
  # TODO(EXP-265): Remove this when fiji-modeling has its own tool launcher.
  modeling_libjars=""
  if [[ -n "${MODELING_HOME}" ]]; then
    modeling_libjars="${MODELING_HOME}/lib/*"
  fi

  # Add FijiMR distribution specific jars.
  if [[ -n "${FIJI_MR_HOME}" && "${FIJI_HOME}" != "${FIJI_MR_HOME}" ]]; then
    mr_libjars="${FIJI_MR_HOME}/lib/*"
    mr_distrodirs="${FIJI_MR_HOME}/lib/distribution/${FIJI_HADOOP_DISTRO_VER}"
    if [ -d "${mr_distrodirs}" ]; then
      mr_distrojars="${mr_distrodirs}/*"
    fi
  fi

  # Add FijiSchema distribution specific jars.
  schema_libjars="${libjars}:${FIJI_HOME}/lib/*"
  schema_distrodir="$FIJI_HOME/lib/distribution/$FIJI_HADOOP_DISTRO_VER"
  if [ -d "${schema_distrodir}" ]; then
    schema_distrojars="${schema_distrodir}/*"
  fi

  # Compose everything together into a classpath.
  libjars="${express_libjars}:${mr_distrojars}:${mr_libjars}:${schema_distrojars}:${schema_libjars}:${schema_shell_libjars}:${modeling_libjars}"

  # Gather the HBase classpath.
  hbase_cp=$(${HBASE_HOME}/bin/hbase classpath)
  hbase_cp=$(mutate_classpath 'slf4j-log4j12' "${hbase_cp}")

  # Hadoop classpath
  hadoop_cp=$(${HADOOP_HOME}/bin/hadoop classpath)
  hadoop_cp=$(mutate_classpath 'slf4j-log4j12' "${hadoop_cp}")

  # Note that we put the libjars before the hbase jars, in case there are conflicts.
  express_conf=${EXPRESS_HOME}/conf
  # We put $libjars_cp at the beginning classpath to allow users to win when there are
  # conflicts.
  express_cp="${libjars_cp}:${express_conf}:${libjars}:${hadoop_cp}:${hbase_cp}"

  # Use parts of the classpath to determine jars to send with jobs through the distributed cache.
  tmpjars_cp="${libjars_cp}:${libjars}"
  tmpjars=$(java -cp ${express_cp} org.fiji.express.tool.TmpJarsTool ${tmpjars_cp})

  # Determine location of Hadoop native libraries and set java.library.path.
  if [ -d "${HADOOP_HOME}/lib/native" ]; then
    JAVA_PLATFORM=`java -cp ${hadoop_cp} -Xmx32m org.apache.hadoop.util.PlatformName | sed -e "s/ /_/g"`
    if [ -d "${HADOOP_HOME}/lib/native/${JAVA_PLATFORM}" ]; then
      # if $HADOOP_HOME/lib/native/$JAVA_PLATFORM exists, use native libs from there.
      if [ ! -z "${JAVA_LIBRARY_PATH}" ]; then
        JAVA_LIBRARY_PATH="${JAVA_LIBRARY_PATH}:${HADOOP_HOME}/lib/native/${JAVA_PLATFORM}"
      else
        JAVA_LIBRARY_PATH="${HADOOP_HOME}/lib/native/${JAVA_PLATFORM}"
      fi
    elif [ -d "${HADOOP_HOME}/lib/native" ]; then
      # If not, check for a global $HADOOP_HOME/lib/native/ and just use that dir.
      if [ ! -z "${JAVA_LIBRARY_PATH}" ]; then
        JAVA_LIBRARY_PATH="${JAVA_LIBRARY_PATH}:${HADOOP_HOME}/lib/native/"
      else
        JAVA_LIBRARY_PATH="${HADOOP_HOME}/lib/native/"
      fi
    fi
  fi
}

function jar_usage() {
  echo "Usage: express jar <jarFile> <mainClass> [args...]"
  echo "       express job <jarFile> <jobClass> [args...]"
}

function jar_command() {
  if [[ ${#} > 0 && ${1} == "--help" ]]; then
    jar_usage
    echo
    exit 0
  fi
  user_target=${1}
  class=${2}
  shift 2
  COMMAND_ARGS=${@}
  if [ -z "${user_target}" ]; then
    echo "Error: no jar file specified."
    jar_usage
    exit 1
  fi
  if [ ! -f "${user_target}" ]; then
    echo "Error: cannot find jar file ${user_target}"
    jar_usage
    exit 1
  fi
  if [ -z "${class}" ]; then
    echo "Error: no main class specified."
    jar_usage
    exit 1
  fi
  express_cp="${user_target}:${express_cp}"
}

function print_tool_usage() {
  echo 'The express script can run programs written using FijiExpress.'
  echo
  echo 'USAGE'
  echo
  echo '  express <command> [--libjars <list of dependency jars separated by colon> <args>'
  echo
  echo 'COMMANDS'
  echo
  echo '  help          - Displays this help message. Use --verbose for more information.'
  echo '  shell         - Starts an interactive shell for running FijiExpress code.'
  echo '  schema-shell  - Starts FijiSchema Shell loaded with FijiExpress extensions.'
  echo '  job           - Runs a compiled FijiExpress job.'
  echo '  jar           - Runs an arbitrary Scala or Java program.'
  echo '  classpath     - Prints the classpath used to run FijiExpress.'
  echo
}

function print_env_usage() {
  echo
  echo "ENVIRONMENT VARIABLES"
  echo
  echo "  Users can set several environment variables to change the behavior of the express"
  echo "  script."
  echo "  These include:"
  echo
  echo "  EXPRESS_JAVA_OPTS   Should contain extra arguments to pass to the JVM used to run"
  echo "                      FijiExpress. By default, EXPRESS_JAVA_OPTS is empty."
  echo
  echo "  FIJI_CLASSPATH      Should contain a colon-separated list of jar files you want to"
  echo "                      add to the classpath."
  echo "                      These files will also get copied to the distributed cache for"
  echo "                      MapReduce tasks that get launched.  Files specified with the"
  echo "                      --libjars flag take precedence over FIJI_CLASSPATH.  By default"
  echo "                      FIJI_CLASSPATH is empty."
  echo
  echo "  JAVA_LIBRARY_PATH   Should contain a colon-separated list of paths to additional native"
  echo "                      libraries to pass to the JVM (through the java.library.path"
  echo "                      property). Note the express script will always pass the native"
  echo "                      libraries included with your Hadoop distribution to the JVM. By"
  echo "                      default JAVA_LIBRARY_PATH is empty."
}

command=${1}

case ${command} in
  help)
    shift
    print_tool_usage
    if [[ ${1} == "--verbose" ]]; then
      print_env_usage
    fi
    exit 0
    ;;

  classpath)
    shift
    extract_classpath "${@}"
    echo "${express_cp}"
    exit 0
    ;;

  job)
    shift  # pop off the command
    extract_classpath "${@}"
    jar_command ${COMMAND_ARGS}
    if [[ "${COMMAND_ARGS}" != *--hdfs* ]] && [[ "${COMMAND_ARGS}" != *--local* ]]; then
      # Default run mode is local.
      run_mode_flag="--local"
    fi
    express_tool="org.fiji.express.flow.ExpressTool"
    ;;

  jar)
    shift  # pop off the command
    extract_classpath "${@}"
    jar_command ${COMMAND_ARGS}
    ;;

  schema-shell)
    shift # pop off command
    # Check if SCHEMA_SHELL_HOME is set. If not we cannot run the shell.
    if [ -z "${SCHEMA_SHELL_HOME}" ]; then
      echo "The environment variable SCHEMA_SHELL_HOME is undefined, and so FijiSchema Shell"
      echo "cannot be run. Please set SCHEMA_SHELL_HOME to the path to a FijiSchema Shell"
      echo "distribution and try again."
      exit 1
    fi
    extract_classpath "${@}"
    schema_shell_script="${SCHEMA_SHELL_HOME}/bin/fiji-schema-shell"
    # We'll add express dependencies to FIJI_CLASSPATH so that they are picked up by
    # fiji-schema-shell.
    export FIJI_CLASSPATH="${FIJI_CLASSPATH}:${express_cp}"
    # Pass tmpjars to fiji-schema-shell using a JVM property, which express's fiji-schema-shell
    # module knows to read and use to populate tmpjars for launched jobs.
    JAVA_OPTS="${JAVA_OPTS} -Dexpress.tmpjars=${tmpjars}"
    # Also specify that schema validation should be disabled.
    JAVA_OPTS="${JAVA_OPTS} -Dorg.fiji.schema.impl.AvroCellEncoder.SCHEMA_VALIDATION=DISABLED"
    export JAVA_OPTS
    # Invoke the fiji-schema-shell command with the express modules preloaded.
    ${schema_shell_script} --modules=modeling ${COMMAND_ARGS}
    exit $?
    ;;

  shell)
    shift # pop off the command

    # Pass a classpath to run the shell with and jars to ship with the distributed cache to
    # the shell runner.
    extract_classpath "${@}"
    export EXPRESS_CP="${express_cp}"
    export TMPJARS="${tmpjars}"

    # Pass a run mode to the shell runner.
    local_mode_script="${bin}/local-mode.scala"
    hdfs_mode_script="${bin}/hdfs-mode.scala"
    if [[ "${COMMAND_ARGS}" == *--hdfs* ]]; then
      export EXPRESS_MODE="${hdfs_mode_script}"
      COMMAND_ARGS=$(echo "${COMMAND_ARGS}" | sed s/--hdfs//)
    elif [[ "${COMMAND_ARGS}" == *--local* ]]; then
      export EXPRESS_MODE="${local_mode_script}"
      COMMAND_ARGS=$(echo "${COMMAND_ARGS}" | sed s/--local//)
    else
      export EXPRESS_MODE="${local_mode_script}"
    fi

    # Run the shell
    "${bin}/express-shell" ${COMMAND_ARGS}
    exit $?
    ;;
  *)
    echo "Unknown command: ${command}"
    echo "Try:"
    echo "  express help"
    exit 1
    ;;
esac

export EXPRESS_JAVA_OPTS

java_opts=
if [ ! -z "${JAVA_LIBRARY_PATH}" ]; then
  java_opts="${java_opts} -Djava.library.path=${JAVA_LIBRARY_PATH}"
fi

# Run it!
if [ -z "${express_tool}" ]; then
  # In this case the user is running an arbitrary jar with express code on the classpath.
  # TODO EXP-243: Until we add a keyword in the express/modeling projects to run a model
  # lifecycle tool, we use `express jar` to run it. You may need to add the following
  # flags for this to work: "-Dtmpjars=${tmpjars}" and
  # "-Dmapreduce.task.classpath.user.precedence=true"
  exec java \
    -cp "${express_cp}" ${java_opts} ${EXPRESS_JAVA_OPTS} \
    "${class}" ${COMMAND_ARGS}
else
  canonical_user_target=$(resolve_symlink ${user_target})
  if [ -z "${canonical_user_target}" ]; then
    echo "File does not exist: ${user_target}"
    exit 1
  fi

  # If the user has put any -Dhadoop.arg=value elements in their arguments ($COMMAND_ARGS),
  # then we need to extract these left-justified arguments from COMMAND_ARGS and add them
  # to HADOOP_ARGS to pass as argments to express_tool to be parsed by Hadoop's
  # GenericOptionsParser. The remaining arguments must be delivered as the final arguments
  # to express_tool after its other arguments.

  # We define and then execute two methods on $COMMAND_ARGS to do this separation.

  function get_hadoop_argv() {
    out=""
    while [ ! -z "${1}" ]; do
      if [ "${1}" == "-D" ]; then
        out="${out} -D ${2}"
        shift # Consume -D
        shift # Consume prop=val
      elif [[ "${1}" == -D* ]]; then
        # Argument matches -Dprop=val.
        # Note [[ eval ]] and lack of "quotes" around -D*.
        out="${out} ${1}"
        shift
      elif [ "${1}" == "-conf" ]; then
        out="${out} -conf ${2}"
        shift # Consume -conf
        shift # Consume <configuration file>
      elif [ "${1}" == "-fs" ]; then
        out="${out} -fs ${2}"
        shift # Consume -fs
        shift # Consume <local|namenode:port>
      elif [ "${1}" == "-jt" ]; then
        out="${out} -jt ${2}"
        shift # Consume -jt
        shift # Consume <local|jobtracker:port>
      elif [ "${1}" == "-archives" ]; then
        out="${out} -archives ${2}"
        shift # Consume -archives
        shift # Consume <comma separated list of jars>
      else
        break # Matched a non -D argument; stop parsing.
      fi
    done

    # Echo all the -Dargs.
    echo "$out"
  }

  # Return the part of COMMAND_ARGS that does not contain leading -D prop=val or -Dprop-val
  function get_user_argv() {
    while [ ! -z "${1}" ]; do
      if [ "${1}" == "-D" ]; then
        shift
        shift # Consume this and the following prop=val
      elif [[ "${1}" == -D* ]]; then
        shift # Consume -Dprop=val
      elif [ "${1}" == "-conf" ]; then
        shift # Consume -conf
        shift # Consume <configuration file>
      elif [ "${1}" == "-fs" ]; then
        shift # Consume -fs
        shift # Consume <local|namenode:port>
      elif [ "${1}" == "-jt" ]; then
        shift # Consume -jt
        shift # Consume <local|jobtracker:port>
      elif [ "${1}" == "-archives" ]; then
        shift # Consume -archives
        shift # Consume <comma separated list of jars>
      else
        break
      fi
    done

    # Echo the remaining args
    echo $*
  }

  hadoop_argv=$(get_hadoop_argv $COMMAND_ARGS)
  user_argv=$(get_user_argv $COMMAND_ARGS)

  if [ -z "${class}" ]; then
    # In this case the user is running an uncompiled script.
    exec java \
      -cp "${express_cp}" ${java_opts} ${EXPRESS_JAVA_OPTS} \
      -Dorg.fiji.schema.impl.AvroCellEncoder.SCHEMA_VALIDATION=DISABLED \
      ${express_tool} \
      "-Dtmpjars=${tmpjars}" \
      "-Dmapreduce.task.classpath.user.precedence=true" \
      ${hadoop_argv} \
      "${canonical_user_target}" \
      "${run_mode_flag}" \
      ${user_argv}
  else
    # In this case the user is running a compiled Scalding Job in a jar.
    exec java \
      -cp "${express_cp}" ${java_opts} ${EXPRESS_JAVA_OPTS} \
      -Dorg.fiji.schema.impl.AvroCellEncoder.SCHEMA_VALIDATION=DISABLED \
      ${express_tool} \
      "-Dtmpjars=file://${canonical_user_target},${tmpjars}" \
      "-Dmapreduce.task.classpath.user.precedence=true" \
      ${hadoop_argv} \
      "${class}" \
      "${run_mode_flag}" \
      ${user_argv}
  fi
fi
