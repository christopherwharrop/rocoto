##########################################
#
# Module WorkflowMgr
#
##########################################
module WorkflowMgr

  ##########################################  
  #
  # Class WorkflowYAMLConfig
  #
  ##########################################
  class WorkflowYAMLConfig

    require 'yaml'
    require 'workflowmgr/forkit'

    DEFAULT_CONFIG={ 
                    :DatabaseType => "SQLite", 
                    :WorkflowDocType => "XML"
                   }

    ##########################################  
    #
    # Initialize
    #
    ##########################################
    def initialize

      # Path to configuration file is $HOME/.wfmrc
      @config_file="#{ENV['HOME']}/.wfmrc"

      # Load the configuration
      begin
        @config=WorkflowMgr.forkit(2) do
          if File.exists?(@config_file)
            config=YAML.load_file(@config_file)
            if config.is_a?(Hash)
              DEFAULT_CONFIG.merge(config)
            else
              raise "Invalid configuration in #{@config_file}"
            end
          else
	    DEFAULT_CONFIG
          end
        end
      rescue WorkflowMgr::ForkitTimeoutException => e
        WorkflowMgr.ioerr(@config_file)
        raise e
      end

    end  # initialize

  end  # Class WorkflowYAMLConfig

end  # Module WorkflowMgr