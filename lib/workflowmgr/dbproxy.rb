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
    require 'workflowmgr/utilities'
    require 'drb'

    ##########################################
    #
    # initialize
    #
    ##########################################
    def initialize(config, options)
      # Store the database creation parameters
      @config = config
      @options = options

      # Initialize the database
      initdb

      # Define the stop! method to increase performance by avoiding calls to method_missing
      (class << self; self; end).instance_eval do
        define_method :stop! do |*args|
          WorkflowMgr.timeout(60) do
            @dbServer.send(:stop!, *args)
          end
        rescue DRb::DRbConnError
          msg = "WARNING! Can't shut down rocotodbserver process #{@dbPID} on host #{@dbHost} because it is not running."
          WorkflowMgr.stderr(msg, 2)
          WorkflowMgr.log(msg)
        rescue Timeout::Error
          msg = "ERROR! Can't shut down rocotodbserver process #{@dbPID} on host #{@dbHost} because it is unresponsive and is probably wedged."
          WorkflowMgr.stderr(msg, 2)
          WorkflowMgr.log(msg)
        end
      end

      # Define methods
      (WorkflowMgr.const_get("Workflow#{@config.DatabaseType}DB").instance_methods - Object.instance_methods).each do |m|
        (class << self; self; end).instance_eval do
          define_method m do |*args|
            retries = 0
            busy_retries = 0.0
            begin
              WorkflowMgr.timeout(150) do
                @dbServer.send(m, *args)
              end
            rescue DRb::DRbConnError
              if retries < 1
                retries += 1
                msg = "WARNING! The rocotodbserver process #{@dbPID} on host #{@dbHost} died.  Attempting to restart and try again."
                WorkflowMgr.stderr(msg, 2)
                WorkflowMgr.log(msg)
                initdb
                retry
              else
                msg = "WARNING! The rocotodbserver process #{@dbPID} on host #{@dbHost} died.  #{retries} attempts to restart the server have failed, giving up."
                raise msg
              end
            rescue WorkflowMgr::WorkflowDBLockedException
              if busy_retries < 10
                busy_retries += 1.0
                sleep 0.5
                retry
              else
                raise
              end
            rescue Timeout::Error
              msg = "WARNING! The rocotodbserver process #{@dbPID} on host #{@dbHost} is unresponsive and is probably wedged."
              raise msg
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
      wfmdir = File.dirname(File.dirname(__dir__))

      # Create the database object
      begin
        # Initialize the database but do not open it (call dbopen to open it)
        database = WorkflowMgr.const_get("Workflow#{@config.DatabaseType}DB").new(@options.database)
        # Skip launching daemon in dryrun mode - no database writes needed
        if @config.DatabaseServer && !WorkflowMgr.dryrun_mode?

          # Ignore SIGINT while launching server process
          Signal.trap("INT", nil)

          # Launch server process
          @dbServer, @dbHost, @dbPID = WorkflowMgr.launchServer("#{wfmdir}/sbin/rocotodbserver")
          @dbServer.setup(database)

          # Restore default SIGINT handler
          Signal.trap("INT", "DEFAULT")

        else
          @dbServer = database
        end
      rescue StandardError => e
        # Try to stop the dbserver if something went wrong
        # Only attempt daemon shutdown when a daemon was actually launched (not in dryrun)
        if @config.DatabaseServer && !WorkflowMgr.dryrun_mode? && !(@dbServer.nil? || !@dbServer.respond_to?(:stop!))
          @dbServer.stop!
        end

        # Raise fatal exception
        WorkflowMgr.stderr(e.message, 1)
        WorkflowMgr.log(e.message)
        if e.is_a?(ArgumentError) || e.is_a?(NameError) || e.is_a?(TypeError)
          WorkflowMgr.stderr(e.backtrace.join("\n"), 1)
          WorkflowMgr.log(e.backtrace.join("\n"))
        end
        raise "ERROR! Could not launch database server process."
      end
    end
  end
end
