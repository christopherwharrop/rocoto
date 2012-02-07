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
                    :DatabaseType => "SQLite3", 
                    :WorkflowDocType => "XML",
                    :DatabaseServer => true,
                    :BatchQueueServer => true,
                    :LogServer => true,
                    :FileStatServer => true,
                    :MaxUnknowns => 3
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


    ##########################################
    #
    # method_missing
    #
    ##########################################
    def method_missing(name,*args)

      configkey=name.to_sym
      if @config.has_key?(configkey)
        return @config[configkey]
      else
        super
      end

    end

  end  # Class WorkflowYAMLConfig

end  # Module WorkflowMgr