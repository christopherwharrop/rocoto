##########################################
#
# module WorkflowMgr
#
##########################################
module WorkflowMgr

  ##########################################
  #
  # Class WorkflowIOProxy
  #
  ##########################################
  class WorkflowIOProxy

    require 'workflowmgr/workflowio'
    require 'system_timer'
    require 'drb'

    ##########################################
    #
    # initialize
    #
    ##########################################
    def initialize(dbServer,config)

      # Store the log creation parameters
      @dbServer=dbServer
      @config=config

      # Get the list of down file paths from the database
      @downpaths=@dbServer.get_downpaths

      # Initialize the workflowIO server
      workflowIO_init

      # Define methods
      (WorkflowIO.instance_methods-Object.instance_methods+[:stop!]).each do |m|

        (class << self; self; end).instance_eval do
          define_method m do |*args|
            retries=0
            begin
              SystemTimer.timeout(60) do       
                @workflowIOServer.send(m,*args)
              end
            rescue DRb::DRbConnError
              if retries < 1
                retries+=1
                puts "*** WARNING! *** WorkflowIO server process died.  Attempting to restart and try again."
                workflowIO_init
                retry
              else
                raise "*** ERROR! *** WorkflowIO server process died.  #{retries} attempts to restart the server have failed, giving up."
              end
            rescue Timeout::Error
              raise "*** ERROR! *** WorkflowIO server process is unresponsive and is probably wedged."
            end

          end
        end
      end

    end


  private

    ##########################################
    #
    # workflowIO_init
    #
    ##########################################
    def workflowIO_init

      # Get the WFM install directory
      wfmdir=File.dirname(File.dirname(File.expand_path(File.dirname(__FILE__))))

      begin

        workflowIO=WorkflowIO.new

        # Set up an object to serve requests for batch queue system services
        if @config.WorkflowIOServer
          @workflowIOServer=WorkflowMgr.launchServer("#{wfmdir}/sbin/workflowioserver")
          @workflowIOServer.setup(workflowIO)
        else
          @workflowIOServer=workflowIO
        end

      rescue

        # Print out the exception message
        puts $!

        # Try to stop the log server if something went wrong
        if @config.WorkflowIOServer
          @workflowIOServer.stop! unless @workflowIOServer.nil?
        end

      end

    end

  end  # Class WorkflowIOProxy

end  # Module WorkflowMgr
