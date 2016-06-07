# Grab and configure the hadoop stack.  This doesn't turn anything on.
include_recipe 'hadoop'
include_recipe 'hadoop::zookeeper_server'
include_recipe 'hadoop::hadoop_hdfs_datanode'
include_recipe 'hadoop::hadoop_hdfs_secondarynamenode'
include_recipe 'hadoop::hadoop_hdfs_namenode'
include_recipe 'hadoop::hadoop_yarn_resourcemanager'
include_recipe 'hadoop::hadoop_yarn_nodemanager'
include_recipe 'hadoop::hbase_regionserver'
include_recipe 'hadoop::hbase_master'

def enable_and_start(service_name)
  service "start-#{service_name}" do
    service_name service_name
    supports status: true, restart: true
    action [:enable, :start]
  end
end

def with_retries(label, command)
  execute label do
    command command
    retries 60
    retry_delay 5
  end
end

enable_and_start 'zookeeper-server'
enable_and_start 'hadoop-hdfs-datanode'
enable_and_start 'hadoop-hdfs-secondarynamenode'

# The directories where the namenode keeps its state
dfs_namenode_name_dirs =
  node['hadoop']['hdfs_site']['dfs.namenode.name.dir'].gsub('file://', '').split(',')

# Format HDFS
execute 'format-hdfs' do
  command 'hdfs namenode -format -nonInteractive'
  user 'hdfs'
  not_if { dfs_namenode_name_dirs.all? { |dir| Dir.exist? "#{dir}/current" } }
end

enable_and_start 'hadoop-hdfs-namenode'

# Wait for an HDFS to be ready to create initial directory structure
with_retries 'ready-to-create-hdfs', 'hdfs dfsadmin -safemode wait && hdfs dfs -ls /'

# Create initial directory structure
execute 'create-hdfs' do
  command '/usr/lib/hadoop/libexec/init-hdfs.sh'
  not_if 'hdfs dfs -ls /tmp'
end

enable_and_start 'hadoop-yarn-resourcemanager'
enable_and_start 'hadoop-yarn-nodemanager'

# Wait for zookeeper to be ready to serve requests
with_retries 'ready-to-serve-zookeeper', %q(bash -c 'hbase zkcli <<<"ls /"')

enable_and_start 'hbase-regionserver'
enable_and_start 'hbase-master'

# Wait for hbase to be ready to serve requests
with_retries 'ready-to-serve-hbase',
  %q(bash -c '! JRUBY_OPTS="" hbase shell <<<list 2>&1 | grep -q ERROR')
