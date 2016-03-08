#!/usr/bin/env python3
# -*- mode: python -*-
# -*- coding: utf-8 -*-
"""Runs the FijiExpress Music tutorial."""

import glob
import logging
import os
import shutil
import sys
import tempfile

# Add the root directory to the Python path if necessary:
__path = os.path.dirname(os.path.dirname(os.path.abspath(sys.argv[0])))
if __path not in sys.path:
  sys.path.append(__path)

from base import base
from fiji import fiji_bento
from fiji import tutorial_test


FLAGS = base.FLAGS
LogLevel = base.LogLevel


class Error(Exception):
  """Errors used in this module."""
  pass


# Horizontal ruler:
LINE = '-' * 80


# ------------------------------------------------------------------------------


FLAGS.AddString(
    name='work_dir',
    help='Working directory.',
)

FLAGS.AddString(
    name='maven_local_repo',
    help='Optional Maven local repository from where to fetch artifacts.',
)

FLAGS.AddString(
    name='maven_remote_repo',
    help='Optional Maven remote repository from where to fetch artifacts.',
)

FLAGS.AddString(
    name='fiji_bento_version',
    default=None,
    help=('Version of FijiBento to download and test against. '
          'For example "1.0.0-rc4" or "1.0.0-rc5-SNAPSHOT". '
          'If not specified, uses the most recent version in the nightly repo.'),
)

FLAGS.AddBoolean(
    name='cleanup_after_test',
    default=True,
    help=('When set, disables cleaning up after test. '
          'Bento cluster stay alive, working directory is not wiped.'),
)

FLAGS.AddBoolean(
    name='help',
    default=False,
    help='Prints a help message.',
)


# ------------------------------------------------------------------------------


class Tutorial(object):
  """Runs the FijiMusic tutorial."""

  def __init__(
      self, work_dir, version,
      maven_local_repo=None,
      maven_remote_repo=None,
      python=False):
    """Initializes the tutorial runner.

    Args:
      work_dir: Working directory where to operate.
      version: Version of FijiBento to test, eg. '1.0.0-rc5-SNAPSHOT'.
      maven_local_repo: Optional local Maven repository.
      maven_remote_repo: Optional remote Maven repository.
    """
    self._work_dir = work_dir
    self._run_id = base.NowMS()
    self._fiji_version = version

    # TODO: inject these in FijiBento
    self._maven_local_repo = maven_local_repo
    self._maven_remote_repo = maven_remote_repo
    self._python = python

    # Initialized in Setup()
    self._fiji_bento = fiji_bento.FijiBento(
        path=os.path.join(self.work_dir, 'fiji-bento-%s' % self._fiji_version),
        version=self._fiji_version,
    )

  @property
  def work_dir(self):
    """Returns the working directory."""
    return self._work_dir

  @property
  def fiji_bento(self):
    """Returns the FijiBento install."""
    return self._fiji_bento

  @property
  def bento_cluster(self):
    """Returns the BentoCluster install."""
    return self.fiji_bento.bento_cluster

  def Setup(self):
    """Initializes the tutorial runner.

    Fetches the FijiBento Maven artifact, unzip it, starts a Bento cluster,
    and prepares a working environment.
    """
    self.fiji_bento.Install()
    self.bento_cluster.Start()

    self._express_music_dir = (
        os.path.join(self.fiji_bento.path, 'examples', 'express-music'))
    assert os.path.exists(self._express_music_dir), (
        'FijiExpress tutorial root directory not found: %r' % self._express_music_dir)

    self._hdfs_base = 'express-music-%d' % self._run_id
    self._fiji_instance_uri = 'fiji://.env/fiji_music_%d' % self._run_id

    express_music_lib_dir = os.path.join(self._express_music_dir, 'lib')

    # Find the jar for fiji-express-music in the lib dir.
    music_jars = glob.glob("{}/{}".format(express_music_lib_dir, "fiji-express-music-*.jar"))
    assert (len(music_jars) == 1)
    express_music_jar = music_jars[0]

    # Builds a working environment for FijiMusic tutorial commands:
    self._env = dict(os.environ)
    self._env.update({
        'MUSIC_EXPRESS_HOME': self._express_music_dir,
        'LIBS_DIR': express_music_lib_dir,
        'EXPRESS_MUSIC_JAR': express_music_jar,
        'KIJI': self._fiji_instance_uri,
        'KIJI_CLASSPATH':
            ':'.join(glob.glob(os.path.join(express_music_lib_dir, '*'))),
        'HDFS_BASE': self._hdfs_base,
    })

  def Command(self, command):
    """Runs a Fiji command-line.

    Args:
      command: Fiji command-line to run as a single string.
    """
    cmd = tutorial_test.FijiCommand(
        command=command,
        work_dir=self.fiji_bento.path,
        env=self._env,
    )
    logging.debug('Exit code: %d', cmd.exit_code)
    if logging.getLogger().level <= LogLevel.DEBUG_VERBOSE:
      logging.debug('Output:\n%s\n%s%s', LINE, cmd.output_text, LINE)
      logging.debug('Error:\n%s\n%s%s', LINE, cmd.error_text, LINE)
    else:
      logging.debug('Output: %r', cmd.output_text)
      logging.debug('Error: %r', cmd.error_text)
    return cmd

  def StripJavaHomeLine(self, output_lines):
      """Strips any line about JAVA_HOME being set, which sometimes happens in the output of a
      `fiji` command, from the output lines of a `fiji` command.

       Args:
         command: Output lines from a `fiji` command.
      """
      if "JAVA_HOME" in output_lines[0]:
        return output_lines[1:]
      else:
        return output_lines

  # ----------------------------------------------------------------------------
  # FijiMusic setup:

  def Part1(self):
    """Runs the setup part of the FijiExpress Music tutorial.

    http://docs.fiji.org/tutorials/express-recommendation/DEVEL/express-setup/
    """

    # --------------------------------------------------------------------------

    install = self.Command('fiji install --fiji=${KIJI}')
    assert (install.exit_code == 0)
    assert ('Successfully created fiji instance: ' in install.output_text)

    # --------------------------------------------------------------------------

    create_table = self.Command(base.StripMargin("""
        |fiji-schema-shell \\
        |    --fiji=${KIJI} \\
        |    --file=${MUSIC_EXPRESS_HOME}/music-schema.ddl \\
        """))
    print(create_table.error_text)
    assert (create_table.exit_code == 0)

    # --------------------------------------------------------------------------

    list_tables = self.Command('fiji ls ${KIJI}')
    assert (list_tables.exit_code == 0)
    assert ('songs' in list_tables.output_text), (
        'Missing table "songs": %s' % list_tables.output_lines)
    assert ('users' in list_tables.output_text), (
        'Missing table "users": %s' % list_tables.output_lines)

    # --------------------------------------------------------------------------

    mkdir = self.Command('hadoop fs -mkdir ${HDFS_BASE}/express-tutorial/')
    assert (mkdir.exit_code == 0)

    copy = self.Command(base.StripMargin("""
        |hadoop fs -copyFromLocal \\
        |    ${MUSIC_EXPRESS_HOME}/example_data/*.json \\
        |    ${HDFS_BASE}/express-tutorial/
        """))
    assert (copy.exit_code == 0)

  # ----------------------------------------------------------------------------
  # FijiExpress Music bulk-importing:

  def Part2(self):
    """Runs the importing part of the FijiExpress Music tutorial.

    http://docs.fiji.org/tutorials/express-recommendation/DEVEL/express-importing-data/
    """

    # --------------------------------------------------------------------------

    cmd = ' '
    if self._python == False:
      cmd = base.StripMargin("""
          |express job \\
          |    ${EXPRESS_MUSIC_JAR} \\
          |    com.moz.fiji.express.music.SongMetadataImporter \\
          |    --libjars "${MUSIC_EXPRESS_HOME}/lib/*" \\
          |    --input ${HDFS_BASE}/express-tutorial/song-metadata.json \\
          |    --table-uri ${KIJI}/songs --hdfs
          """)

    else:
      cmd = base.StripMargin("""
          |express.py \\
          |    job \\
          |    --libjars="${MUSIC_EXPRESS_HOME}/lib/*" \\
          |    --user_jar=${EXPRESS_MUSIC_JAR} \\
          |    --job_name=com.moz.fiji.express.music.SongMetadataImporter \\
          |    --mode=hdfs \\
          |    --input ${HDFS_BASE}/express-tutorial/song-metadata.json \\
          |    --table-uri ${KIJI}/songs
          """)

    songMetadataImport = self.Command(cmd)
    assert (songMetadataImport.exit_code == 0)

    # --------------------------------------------------------------------------

    list_rows = self.Command('fiji scan ${KIJI}/songs --max-rows=5')
    assert (list_rows.exit_code == 0)
    # Strip the first line from the output, if it is about $JAVA_HOME not set.
    stripped_output = self.StripJavaHomeLine(list_rows.output_lines)
    assert (stripped_output[0].startswith('Scanning fiji table: fiji://'))
    assert (len(stripped_output) >= 3 * 5 + 1), len(stripped_output)
    for row in range(0, 5):
      tutorial_test.ExpectRegexMatch(
          expect=r"^entity-id=\['song-\d+'\] \[\d+\] info:metadata$",
          actual=stripped_output[1 + row * 3])
      tutorial_test.ExpectRegexMatch(
          expect=r"^\s*{\s*\"song_name\".*\"album_name\".*\"artist_name\".*\"genre\".*\"tempo\".*\"duration\".*\s*}\s*$",
          actual=stripped_output[2 + row * 3])
      tutorial_test.ExpectRegexMatch(
          expect=r"^$",
          actual=stripped_output[3 + row * 3])

    # --------------------------------------------------------------------------

    cmd = ' '
    if self._python == False:
      cmd = base.StripMargin("""
          |express job \\
          |    ${EXPRESS_MUSIC_JAR} \\
          |    com.moz.fiji.express.music.SongPlaysImporter \\
          |    --libjars "${MUSIC_EXPRESS_HOME}/lib/*" \\
          |    --input ${HDFS_BASE}/express-tutorial/song-plays.json \\
          |    --table-uri ${KIJI}/users --hdfs
          """)
    else:
      cmd = base.StripMargin("""
        |express.py \\
        |    job \\
        |    -libjars="${MUSIC_EXPRESS_HOME}/lib/*" \\
        |    -user_jar=${EXPRESS_MUSIC_JAR} \\
        |    -job_name=com.moz.fiji.express.music.SongPlaysImporter \\
        |    -mode=hdfs \\
        |    --input ${HDFS_BASE}/express-tutorial/song-plays.json \\
        |    --table-uri ${KIJI}/users
        """)
    userDataImport = self.Command(cmd)
    assert (userDataImport.exit_code == 0)

    # --------------------------------------------------------------------------

    list_rows = self.Command('fiji scan ${KIJI}/users --max-rows=5')
    assert (list_rows.exit_code == 0)
    stripped_output = self.StripJavaHomeLine(list_rows.output_lines)
    assert (stripped_output[0].startswith('Scanning fiji table: fiji://'))
    assert (len(stripped_output) >= 3 * 5 + 1), len(stripped_output)
    for row in range(0, 5):
      tutorial_test.ExpectRegexMatch(
          expect=r"^entity-id=\['user-\d+'\] \[\d+\] info:track_plays$",
          actual=stripped_output[1 + row * 3])
      tutorial_test.ExpectRegexMatch(
          expect=r"^\s*song-\d+$",
          actual=stripped_output[2 + row * 3])
      tutorial_test.ExpectRegexMatch(
          expect=r"^$",
          actual=stripped_output[3 + row * 3])

  # --------------------------------------------------------------------------
  # play-count section.

  def Part3(self):
    cmd = ' '
    if self._python == False:
      cmd = base.StripMargin("""
        |express job \\
        |  ${EXPRESS_MUSIC_JAR} \\
        |  com.moz.fiji.express.music.SongPlayCounter \\
        |  --libjars "${MUSIC_EXPRESS_HOME}/lib/*" \\
        |  --table-uri ${KIJI}/users \\
        |  --output ${HDFS_BASE}/express-tutorial/songcount-output \\
        |  --hdfs
        """)
    else:
      cmd = base.StripMargin("""
        |express.py \\
        |    job \\
        |    -libjars="${MUSIC_EXPRESS_HOME}/lib/*" \\
        |    -user_jar=${EXPRESS_MUSIC_JAR} \\
        |    -job_name=com.moz.fiji.express.music.SongPlayCounter \\
        |    -mode=hdfs \\
        |    --table-uri ${KIJI}/users \\
        |    --output ${HDFS_BASE}/express-tutorial/songcount-output \\
        """)
    play_count = self.Command(cmd)
    assert (play_count.exit_code == 0)
    fs_text = self.Command("""
        hadoop fs -text ${HDFS_BASE}/express-tutorial/songcount-output/part-00000 | head -3
        """)
    tutorial_test.Expect(expect=0, actual=fs_text.exit_code)
    lines = list(filter(None, self.StripJavaHomeLine(fs_text.output_lines)))  # filter empty lines
    tutorial_test.Expect(expect=3, actual=len(lines))
    for line in lines:
      tutorial_test.ExpectRegexMatch(expect=r'^song-\d+\t\d+$', actual=line)

  # ----------------------------------------------------------------------------
  # Top Next Songs section.
  def Part4(self):
    cmd = ' '
    if self._python == False:
      cmd = base.StripMargin("""
        |express job \\
        |    ${EXPRESS_MUSIC_JAR} \\
        |    com.moz.fiji.express.music.TopNextSongs \\
        |    --libjars "${MUSIC_EXPRESS_HOME}/lib/*" \\
        |    --users-table ${KIJI}/users \\
        |    --songs-table ${KIJI}/songs --hdfs
        """)
    else:
      cmd = base.StripMargin("""
        |express.py \\
        |    job \\
        |    -libjars="${MUSIC_EXPRESS_HOME}/lib/*" \\
        |    -user_jar=${EXPRESS_MUSIC_JAR} \\
        |    -job_name=com.moz.fiji.express.music.TopNextSongs \\
        |    -mode=hdfs \\
        |    --users-table ${KIJI}/users \\
        |    --songs-table ${KIJI}/songs --hdfs
        """)
    top_songs = self.Command(cmd)
    assert (top_songs.exit_code == 0)
    list_rows = self.Command('fiji scan ${KIJI}/songs --max-rows=2')
    assert (list_rows.exit_code == 0)
    stripped_output = self.StripJavaHomeLine(list_rows.output_lines)
    assert (stripped_output[0].startswith('Scanning fiji table: fiji://'))
    assert (len(stripped_output) >= 5 * 2 + 1), len(stripped_output)
    for row in range(0, 2):
      tutorial_test.ExpectRegexMatch(
          expect=r"^entity-id=\['song-\d+'\] \[\d+\] info:metadata$",
          actual=stripped_output[1 + row * 5])
      tutorial_test.ExpectRegexMatch(
          expect=r"^\s*{\s*\"song_name\".*\"album_name\".*\"artist_name\".*\"genre\".*\"tempo\".*\"duration\".*\s*}\s*$",
          actual=stripped_output[2 + row * 5])
      tutorial_test.ExpectRegexMatch(
          expect=r"^entity-id=\['song-\d+'\] \[\d+\] info:top_next_songs$",
          actual=stripped_output[3 + row * 5])
      tutorial_test.ExpectRegexMatch(
          expect=r"^\s*{\s*\"top_songs\".*}$",
          actual=stripped_output[4 + row * 5])
      tutorial_test.ExpectRegexMatch(
          expect=r"^$",
          actual=stripped_output[5 + row * 5])

  # ----------------------------------------------------------------------------
  # Song recommender
  def Part5(self):
    cmd = ' '
    if self._python == False:
      cmd = base.StripMargin("""
        |express job ${EXPRESS_MUSIC_JAR} \\
        |    com.moz.fiji.express.music.SongRecommender \\
        |    --songs-table ${KIJI}/songs \\
        |    --users-table ${KIJI}/users
        """)
    else:
      cmd = base.StripMargin("""
        |express.py \\
        |    job \\
        |    -user_jar=${EXPRESS_MUSIC_JAR} \\
        |    -job_name=com.moz.fiji.express.music.SongRecommender \\
        |    -mode=hdfs \\
        |    --songs-table ${KIJI}/songs \\
        |    --users-table ${KIJI}/users
        """)
    song_recommend = self.Command(cmd)
    assert (song_recommend.exit_code == 0)

    list_rows = self.Command("fiji scan ${KIJI}/users --max-rows=2")
    assert (list_rows.exit_code == 0)
    stripped_output = self.StripJavaHomeLine(list_rows.output_lines)
    assert (stripped_output[0].startswith('Scanning fiji table: fiji://'))
    assert (len(stripped_output) >= 5 * 2 + 1), len(stripped_output)
    for row in range(0, 2):
      tutorial_test.ExpectRegexMatch(
          expect=r"^entity-id=\['user-\d+'\] \[\d+\] info:track_plays$",
          actual=stripped_output[1 + row * 5])
      tutorial_test.ExpectRegexMatch(
          expect=r"^\s*song-\d+$",
          actual=stripped_output[2 + row * 5])
      tutorial_test.ExpectRegexMatch(
          expect=r"^entity-id=\['user-\d+'\] \[\d+\] info:next_song_rec$",
          actual=stripped_output[3 + row * 5])
      tutorial_test.ExpectRegexMatch(
          expect=r"^\s*song-\d+$",
          actual=stripped_output[4 + row * 5])
      tutorial_test.ExpectRegexMatch(
          expect=r"^$",
          actual=stripped_output[5 + row * 5])


  # ----------------------------------------------------------------------------
  # Cleanup:

  def Cleanup(self):
    self.bento_cluster.Stop()
    shutil.rmtree(self.work_dir)


# ------------------------------------------------------------------------------

def Main(args):
  """Program entry point."""
  if FLAGS.help:
    FLAGS.PrintUsage()
    return os.EX_OK

  if len(args) > 0:
    logging.error('Unexpected command-line arguments: %r' % args)
    FLAGS.PrintUsage()
    return os.EX_USAGE

  # Create a temporary working directory:
  cwd = os.getcwd()
  work_dir = FLAGS.work_dir
  if work_dir is None:
    work_dir = tempfile.mkdtemp(prefix='work_dir.', dir=os.getcwd())
  work_dir = os.path.abspath(work_dir)
  if not os.path.exists(work_dir):
    os.makedirs(work_dir)
  FLAGS.work_dir = work_dir

  logging.info('Working directory: %r', work_dir)

  logging.info('Testing tutorial of FijiBento %s', FLAGS.fiji_bento_version)

  # Runs the tutorial:
  deprecatedTutorial = Tutorial(
      work_dir=work_dir,
      version=FLAGS.fiji_bento_version,
      maven_local_repo=FLAGS.maven_local_repo,
      maven_remote_repo=FLAGS.maven_remote_repo,
      python=False
  )

  try:
    deprecatedTutorial.Setup()
    deprecatedTutorial.Part1()
    deprecatedTutorial.Part2()
    deprecatedTutorial.Part3()
    deprecatedTutorial.Part4()
    deprecatedTutorial.Part5()
  finally:
    if FLAGS.cleanup_after_test:
      deprecatedTutorial.Cleanup()

  pythonTutorial = Tutorial(
      work_dir=work_dir,
      version=FLAGS.fiji_bento_version,
      maven_local_repo=FLAGS.maven_local_repo,
      maven_remote_repo=FLAGS.maven_remote_repo,
      python=True
  )

  try:
    pythonTutorial.Setup()
    pythonTutorial.Part1()
    pythonTutorial.Part2()
    pythonTutorial.Part3()
    pythonTutorial.Part4()
    pythonTutorial.Part5()
  finally:
    if FLAGS.cleanup_after_test:
      pythonTutorial.Cleanup()


# ------------------------------------------------------------------------------


if __name__ == '__main__':
  base.Run(Main)
