Vagrant.configure('2') do |config|
  config.vm.box = 'ubuntu/trusty64'
  config.vm.hostname = 'fiji'
  config.ssh.forward_agent = true
  # config.ssh.forward_x11 = true

  #config.vm.network 'forwarded_port', guest: 50070, host: 50070 # Namenode
  #config.vm.network 'forwarded_port', guest: 50075, host: 50075 # Datanode
  #config.vm.network 'forwarded_port', guest: 50090, host: 50090 # 2ndary Namenode
  #config.vm.network 'forwarded_port', guest: 60010, host: 60010 # HBase Master
  #config.vm.network 'forwarded_port', guest: 60030, host: 60030 # Region Server
  #config.vm.network 'forwarded_port', guest: 8088, host: 8088 # Resource Manager
  #config.vm.network 'forwarded_port', guest: 8042, host: 8042 # Node Manager

  # vagrant plugin install vagrant-cachier
  # http://fgrehm.viewdocs.io/vagrant-cachier/usage/
  if Vagrant.has_plugin?('vagrant-cachier')
    config.cache.scope = :box
  end

  %w(vmware_fusion vmware_workstation).each do |vmware|
    config.vm.provider vmware do |provider, override|
      # provider.gui = true
      override.vm.box = 'netsensia/ubuntu-trusty64'
      provider.vmx['memsize'] = '3072'  # VMware refuses to start for anything larger
      provider.vmx['numvcpus'] = '1'    # http://superuser.com/q/505711/96477
    end
  end

  config.vm.provider 'virtualbox' do |provider, override|
    # provider.gui = true
    provider.name = 'fiji'
    provider.customize ['modifyvm', :id, '--memory', '3135'] # shrinks to 3072 inside
    provider.customize ['modifyvm', :id, '--cpus', '2']
  end

  # Enables the chef provisioner below.
  config.vm.provision :shell, inline: %(
which chef || wget -qO - https://www.chef.io/chef/install.sh | bash -s -- -P chefdk
berks vendor -b /vagrant/Berksfile /tmp/vagrant-chef/cookbooks
)

  config.vm.provision :chef_solo do |chef_solo|
    chef_solo.install = false
    chef_solo.binary_path = '/opt/chefdk/bin'

    hadoop = {}

    if ENV.key?('FIJI_HADOOP_DISTRIBUTION')
      hadoop['distribution'] = ENV['FIJI_HADOOP_DISTRIBUTION']
    end

    if ENV.key?('FIJI_HADOOP_DISTRIBUTION_VERSION')
      hadoop['distribution_version'] = ENV['FIJI_HADOOP_DISTRIBUTION_VERSION']
    end

    chef_solo.json = {
      "hadoop" => hadoop
    }

    chef_solo.add_recipe 'fiji::development'
  end
end
