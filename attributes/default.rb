default['java'].tap do |java|
  java['install_flavor'] = 'oracle'
  java['jdk_version'] = '8'
  java['oracle']['accept_oracle_download_terms'] = true
end

default['hadoop'].tap do |hadoop|
  hadoop['distribution'] = 'cdh'
  hadoop['distribution_version'] = '5.3.5'
end

default['hadoop']['hdfs_site'].tap do |hdfs_site|
  # Removing causes the hadoop hbase recipe validations to eagerly fail
  hdfs_site['dfs.datanode.max.transfer.threads'] = 4096
end

default['hbase']['mapred_site'].tap do |mapred_site|
  mapred_site['mapreduce.framework.name'] = 'yarn'
end

default['hbase']['hbase_site'].tap do |hbase_site|
  hbase_site['hbase.cluster.distributed'] = true
  hbase_site['hbase.zookeeper.quorum'] = 'localhost'
end
