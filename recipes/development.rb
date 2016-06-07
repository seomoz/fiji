# Grab git
include_recipe 'git'

# Install python
# These are the packages recommended by https://github.com/yyuu/pyenv/wiki
package 'build-essential'
package 'curl'
package 'libbz2-dev'
package 'libncurses5-dev'
package 'libreadline-dev'
package 'libsqlite3-dev'
package 'libssl-dev'
package 'llvm'
package 'make'
package 'wget'
package 'zlib1g-dev'

git 'checkout-pyenv' do
  destination '/home/vagrant/.pyenv'
  repository 'https://github.com/yyuu/pyenv.git'
  revision '0c4392bf16bf1e7a54b21e417979591717fb7ef3'
  user 'vagrant'
  group 'vagrant'
end

ruby_block 'initialize pyenv on login' do
  block do
    profile = Chef::Util::FileEdit.new('/home/vagrant/.profile')
    profile.insert_line_if_no_match(
      /export PYENV_ROOT/,
      <<-'EOF'

# Load pyenv
export PYENV_ROOT="$HOME/.pyenv"
export PATH="$PYENV_ROOT/bin:$PATH"
eval "$(pyenv init -)"
      EOF
    )
    profile.write_file
  end
end

execute 'install-python' do
  command 'sudo -u vagrant --login pyenv install 3.4.3 --skip-existing'
end

execute 'set-python' do
  command 'sudo -u vagrant --login pyenv local 3.4.3'
end

# Grab maven.  Do this before java to ensure we get the java we want.
package 'maven'

# Grab java
include_recipe 'java'

# Color shell prompts
execute "sed -i 's|#force_color|force_color|' /home/vagrant/.bashrc"

# Make a symlink to /vagrant
link '/home/vagrant/fiji' do
  to '/vagrant'
end

include_recipe 'fiji::common'
