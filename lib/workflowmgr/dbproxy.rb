##########################################
#
# module WorkflowMgr
#
##########################################
module WorkflowMgr

  ##########################################
  #
  # Class DBProxy
  #
  ##########################################
  class DBProxy

    require 'workflowmgr/workflowdb'
    require 'system_timer'
    require 'drb'

    ##########################################
    #
    # initialize
    #
    ##########################################
    def initialize(dbFile,config)

      # Store the database creation parameters
      @dbFile=dbFile
      @config=config

      # Initialize the database
      initdb

    end

    ##########################################
    #
    # method_missing
    #
    ##########################################
    def method_missing(name,*args)

      retries=0
      begin
        SystemTimer.timeout(60) do
          return @dbServer.send(name,*args)
        end
      rescue DRb::DRbConnError
        if retries < 1
          retries+=1
          puts "*** WARNING! *** Database server process died.  Attempting to restart and try again."
          initdb
          retry
        else
          raise "*** ERROR! *** Database server process died.  #{retries} attempts to restart the server have failed, giving up."
	end
      rescue Timeout::Error
        raise "*** ERROR! *** Database server process is unresponsive and is probably wedged"
      end

    end


  private

    ##########################################
    #
    # initdb
    #
    ##########################################
    def initdb

      # Get the WFM install directory
      wfmdir=File.dirname(File.dirname(File.expand_path(File.dirname(__FILE__))))

      # Create the database object
      begin

	# Initialize the database but do not open it (call dbopen to open it)
	database=WorkflowMgr::const_get("Workflow#{@config.DatabaseType}DB").new(@dbFile)

	if @config.DatabaseServer
          @dbServer=WorkflowMgr.launchServer("#{wfmdir}/sbin/workflowdbserver")
          @dbServer.setup(database)
	else
          @dbServer=database
        end

      rescue

        # Print out the exception message
	puts $!

        # Try to stop the dbserver if something went wrong
	if @config.DatabaseServer
          @dbServer.stop! unless @dbServer.nil?
        end

      end

    end

  end  # Class DBProxy

end  # Module WorkflowMgr
