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
    require 'fileutils'

    DEFAULT_CONFIG={ 
                    :DatabaseType => "SQLite3", 
                    :WorkflowDocType => "XML",
                    :DatabaseServer => true,
                    :BatchQueueServer => true,
                    :WorkflowIOServer => true,
                    :MaxUnknowns => 3,
                    :MaxLogDays => 7,
                    :AutoVacuum => true,
                    :VacuumPurgeDays => 30
                   }

    ##########################################  
    #
    # Initialize
    #
    ##########################################
    def initialize

      # Path to configuration file is $HOME/.rocoto/rocotorc
      @config_dir="#{ENV['HOME']}/.rocoto"
      @config_tmp="#{@config_dir}/tmp"
      @config_file="#{@config_dir}/rocotorc"

      # Load the configuration
      begin
        @config=WorkflowMgr.forkit(10) do

          # Create a .rocoto directory if one does not already exist
          FileUtils.mkdir_p(@config_dir) unless File.exists?(@config_dir)

          # Create a .rocoto tmp dir if one does not already exist
          FileUtils.mkdir_p(@config_tmp) unless File.exists?(@config_tmp)
          
          # Move the legacy .wfmrc file to rocotorc file if it exists
          FileUtils.mv("#{ENV['HOME']}/.wfmrc",@config_file) if File.exists?("#{ENV['HOME']}/.wfmrc")

          # Load the rocotorc config if one exists
          if File.exists?(@config_file)
            config=YAML.load_file(@config_file)
            if config.is_a?(Hash)
              # Merge default config into rocotorc config if there are unspecified config options
              if config.keys.collect {|c| c.to_s}.sort != DEFAULT_CONFIG.keys.collect {|c| c.to_s}.sort
                config=DEFAULT_CONFIG.merge(config).delete_if { |k,v| !DEFAULT_CONFIG.has_key?(k) }

                File.open(@config_file,"w") { |f| YAML.dump(config,f) }
              end  
              config
            else
              raise "Invalid configuration in #{@config_file}"
            end
          else
            # Create a rocotorc file with default settings if it does not exist
            File.open(@config_file,"w") { |f| YAML.dump(DEFAULT_CONFIG,f) }
	    DEFAULT_CONFIG
          end

        end  # @config do

      rescue WorkflowMgr::ForkitTimeoutException
        msg="ERROR: An I/O operation timed out while reading, writing, or testing for the existence of '#{@config_file}'"
        WorkflowMgr.log(msg)
        raise msg
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
