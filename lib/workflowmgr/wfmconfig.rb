##########################################
#
# Module WorkflowMgr
#
##########################################
module WorkflowMgr

  ##########################################  
  #
  # Class WFMYAMLConfig
  #
  ##########################################
  class WFMYAMLConfig

    require 'yaml'
    require 'workflowmgr/forkit'

    DEFAULT_CONFIG={ :DatabaseType => "SQLite" }

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
        @config=forkit(1) do
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
      end

    end  # initialize

  end  # Class WFMYAMLConfig

end  # Module WorkflowMgr