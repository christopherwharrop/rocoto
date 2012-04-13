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

      # Define the stop! method to increase performance by avoiding calls to method_missing
      (class << self; self; end).instance_eval do
        define_method :stop! do |*args|
          begin
            SystemTimer.timeout(30) do
              @dbServer.send(:stop!,*args)
            end
          rescue DRb::DRbConnError
            puts "*** WARNING! *** Can't shut down WorkflowDB server process because it is not running."
          rescue Timeout::Error
            puts "*** ERROR! ***  Can't shut down WorkflowDB server process because it is unresponsive and is probably wedged."
          end
        end
      end

      # Define methods
      (WorkflowMgr::const_get("Workflow#{@config.DatabaseType}DB").instance_methods-Object.instance_methods).each do |m|
        (class << self; self; end).instance_eval do
          define_method m do |*args|
            retries=0
            busy_retries=0
            begin
              SystemTimer.timeout(60) do
                @dbServer.send(m,*args)
              end
            rescue DRb::DRbConnError
              if retries < 1
                retries+=1
                puts "*** WARNING! *** WorkflowDB server process died.  Attempting to restart and try again."
                initdb
                retry
              else
                raise "*** ERROR! *** WorkflowDB server process died.  #{retries} attempts to restart the server have failed, giving up."
              end
            rescue WorkflowMgr::WorkflowDBLockedException
              if busy_retries < 60
                busy_retries+=1
                sleep(rand)
                retry
              else
                raise "*** ERROR! *** WorkflowDB is locked.  #{busy_retries} attempts to access the database have failed, giving up.\n#{$!}"
              end            
            rescue Timeout::Error
              raise "*** ERROR! *** WorkflowDB server process is unresponsive and is probably wedged."
            end

          end
        end
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

          # Ignore SIGINT while launching server process
          Signal.trap("INT",nil)

          # Launch server process
          @dbServer,@dbHost,@dbPID=WorkflowMgr.launchServer("#{wfmdir}/sbin/workflowdbserver")
          @dbServer.setup(database)

          # Restore default SIGINT handler
          Signal.trap("INT","DEFAULT")

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
