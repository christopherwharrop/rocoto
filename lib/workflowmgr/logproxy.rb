##########################################
#
# module WorkflowMgr
#
##########################################
module WorkflowMgr

  ##########################################
  #
  # Class LogProxy
  #
  ##########################################
  class LogProxy

    require 'workflowmgr/workflowlog'
    require 'system_timer'
    require 'drb'

    ##########################################
    #
    # initialize
    #
    ##########################################
    def initialize(log,config)

      # Store the log creation parameters
      @log=log
      @config=config

      # Initialize the log
      initlog

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
          return @logServer.send(name,*args)
        end
      rescue DRb::DRbConnError
        if retries < 1
          retries+=1
          puts "*** WARNING! *** Log server process died.  Attempting to restart and try again."
          initlog
          retry
        else
          raise "*** ERROR! *** Log server process died.  #{retries} attempts to restart the server have failed, giving up."
	end
      rescue Timeout::Error
        raise "*** ERROR! *** Log server process is unresponsive and is probably wedged."
      end

    end

  private

    ##########################################
    #
    # initlog
    #
    ##########################################
    def initlog

      # Get the WFM install directory
      wfmdir=File.dirname(File.dirname(File.expand_path(File.dirname(__FILE__))))

      begin

        # Set up an object to serve requests for batch queue system services
        if @config.LogServer

          # Ignore SIGINT while launching server process
          Signal.trap("INT",nil)

          @logServer=WorkflowMgr.launchServer("#{wfmdir}/sbin/workflowlogserver")
          @logServer.setup(@log)

          # Restore default SIGINT handler
          Signal.trap("INT","DEFAULT")

        else
          @logServer=@log
        end

      rescue

        # Print out the exception message
        puts $!

        # Try to stop the log server if something went wrong
        if @config.LogServer
          @logServer.stop! unless @logServer.nil?
        end

      end

    end

  end  # Class LogProxy

end  # Module WorkflowMgr
