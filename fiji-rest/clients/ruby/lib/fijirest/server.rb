# (c) Copyright 2013 WibiData, Inc.
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

require "fijirest/version"
require "fijirest/client"
require 'yaml'
require 'open-uri'

module FijiRest

  # Class that wraps the starting/stopping of a FijiREST server.
  # assumes that this is running as a non-privileged user that
  # has permissions to read/write to the location where FIJI_REST is
  # installed
  class Server
    FIJI_REST_HOME = "/opt/wibi/fiji-rest"
    def initialize(fiji_rest_home = FIJI_REST_HOME)
      @fiji_server_location = fiji_rest_home
      unless authorized_to_run?
        raise "#{ENV['USER']} not authorized to run FijiREST server!"
      end
      @http_port = 8080
      reload_configuration!
    end

    # Start a server given a zookeeper quorum and array of visible
    # instances. This will modify the configuration.yml and launch the server.
    # param: zk_quorum is the list of zookeeper servers. Default is .env
    # param: visible_instances is an array of instance names that are to be visible by clients
    #        of the REST server.
    # param: wait_for_load specifies whether or not to block until the server has come up. If the
    #        server fails to come up after some period of time, an exception will be raised.
    def start(zk_quorum = ".env", visible_instances = ["default"], wait_for_load = false)
      if running?
        raise "FijiREST appears to be running."
      else
        #Update the configuration file to reflect the desired options
        dropwizard_config = YAML.load_file(qualify_file("/conf/configuration.yml"))

        #Do a bit of string checking so that we aren't adding fiji:// prefix twice.
        prefix="fiji://"
        prefix = "" if zk_quorum[0..6] == "fiji://"

        fiji_uri = "#{prefix}#{zk_quorum}"
        dropwizard_config["cluster"] = fiji_uri
        dropwizard_config["instances"] = visible_instances
        f = File.new(qualify_file("/conf/configuration.yml"), "w")
        f.puts(dropwizard_config.to_yaml)
        f.close
        #Now start the service
        launch_command = qualify_file("/bin/fiji-rest start")
        %x{#{launch_command}}
        if wait_for_load
          (1..20).each {|i|
            break if running?
            sleep 2
            }
          unless running?
            raise "FijiREST failed to start!"
          end
        end
      end
    end

    # Stops the server optionally waiting for the server to shutdown
    # param: wait_for_shutdown determines whether or not to wait for the server to have shutdown
    #        before returning.
    def stop(wait_for_shutdown = false)
      pid_file = qualify_file("fiji-rest.pid")
      launch_command = qualify_file("/bin/fiji-rest stop")
      if File.exist?(pid_file)
        %x{#{launch_command}}
        if wait_for_shutdown
          (1..20).each {|i|
            break unless running?
            sleep 2
            }
           if running?
             raise "FijiREST failed to stop."
           end
        end
      else
        raise "FijiREST not running!"
      end
    end

    # Returns a new client instance.
    def new_client
      Client.new("http://localhost:#{@http_port}")
    end

    private

    # Reloads the configuration.yml storing the configuration in a private member
    # variable.
    def reload_configuration!
      @dropwizard_config = YAML.load_file(qualify_file("/conf/configuration.yml"))
      @http_port = 8080
      if @dropwizard_config.include?("http") && @dropwizard_config["http"].include?("port")
          @http_port = @dropwizard_config["http"]["port"]
      end
    end

    # Checks if this current user is authorized to run the server. A simple check
    # is to ensure that the current user is the owner of a known file (conf/configuration.yml)
    def authorized_to_run?
      File.owned?(qualify_file("/conf/configuration.yml"))
    end

    # Checks if the application is running by hitting a known URI
    def app_running?
      begin
        f = open("http://localhost:#{@http_port}/v1")
        f.close
        true
      rescue
        false
      end
    end

    # Checks if the pid file exists. This will check for the pid file generated
    # when the server is launched by a non-privileged user as well as the pid file
    # generated when this server is launched by /sbin/service.
    def pid_exists?
      local_pid_file = qualify_file("fiji-rest.pid")
      global_pid_file = "/var/run/fiji-rest/fiji-rest.pid"
      File.exists?(local_pid_file) || File.exists?(global_pid_file)
    end

    # Checks if the server is running by checking the process file (fiji-rest.pid)
    # or the global/root run /var/run/fiji-rest.pid
    def running?
      app_running? && pid_exists?
    end

    # Returns a fully qualified filename prefixed by the location of fiji_rest_server
    def qualify_file(filename)
      "#{@fiji_server_location}/#{filename}"
    end
  end
end
