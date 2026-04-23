##########################################
#
# module WorkflowMgr
#
##########################################
module WorkflowMgr
  ##########################################
  #
  # Class BQSProxy
  #
  ##########################################
  class BQSProxy
    require 'workflowmgr/utilities'
    require 'workflowmgr/bqs'
    require 'drb'
    require 'workflowmgr/workflowoption'
    require 'workflowmgr/workflowconfig'

    ##########################################
    #
    # initialize
    #
    ##########################################
    def initialize(batch_system, config, options)
      # Store the batch system proxy creation parameters
      @batchSystem = batch_system
      @config = config
      @options = options

      # Initialize the batch system server
      initbqs

      # Define the stop! method to increase performance by avoiding calls to method_missing
      (class << self; self; end).instance_eval do
        define_method :stop! do |*args|
          WorkflowMgr.timeout(30) do
            @bqServer.send(:stop!, *args)
          end
        rescue DRb::DRbConnError
          msg = "WARNING! Can't shut down rocotobqserver process #{@bqPID} on host #{@bqHost} " \
                "because it is not running."
          WorkflowMgr.stderr(msg, 2)
          WorkflowMgr.log(msg)
        rescue Timeout::Error
          msg = "WARNING! Can't shut down rocotobqserver process #{@bqPID} on host #{@bqHost} " \
                "because it is unresponsive and is probably wedged."
          WorkflowMgr.stderr(msg, 2)
          WorkflowMgr.log(msg)
        end
      end

      # Define methods to increase performance by avoiding calls to method_missing
      (WorkflowMgr.const_get("BQS").instance_methods - Object.instance_methods + [:__drburi]).each do |m|
        (class << self; self; end).instance_eval do
          define_method m do |*args|
            retries = 0
            begin
              WorkflowMgr.timeout(150) do
                @bqServer.send(m, *args)
              end
            rescue DRb::DRbConnError
              if retries < 1
                retries += 1
                msg = "WARNING! The rocotobqserver process #{@bqPID} on host #{@bqHost} died.  " \
                      "Attempting to restart and try again."
                WorkflowMgr.stderr(msg, 2)
                WorkflowMgr.log(msg)
                initbqs
                retry
              else
                msg = "WARNING! The rocotobqserver process #{@bqPID} on host #{@bqHost} died.  " \
                      "#{retries} attempts to restart the server have failed, giving up."
                raise msg
              end
            rescue Timeout::Error
              msg = "WARNING! The rocotobqserver process #{@bqPID} on host #{@bqHost} is unresponsive " \
                    "and is probably wedged."
              raise msg
            end
          end
        end
      end
    end

    private

    ##########################################
    #
    # initbqs
    #
    ##########################################
    def initbqs
      # Get the WFM install directory
      wfmdir = File.dirname(File.dirname(__dir__))

      # Create the batch queue system object
      begin
        # Initialize the
        bqs = BQS.new(@batchSystem, @options.database, @config)

        # Skip launching daemon in dryrun mode - no batch system interaction needed
        if @config.BatchQueueServer && !WorkflowMgr.dryrun_mode?

          # Ignore SIGINT while launching server process
          Signal.trap("INT", nil)

          # Launch server process
          @bqServer, @bqHost, @bqPID = WorkflowMgr.launchServer("#{wfmdir}/sbin/rocotobqserver")
          @bqServer.setup(bqs)

          # Restore default SIGINT handler
          Signal.trap("INT", "DEFAULT")

        else
          @bqServer = bqs
        end
      rescue StandardError => e
        # Try to stop the bqserver if something went wrong
        # Only attempt daemon shutdown when a daemon was actually launched (not in dryrun)
        if @config.BatchQueueServer && !WorkflowMgr.dryrun_mode? && !(@bqServer.nil? || !@bqServer.respond_to?(:stop!))
          @bqServer.stop!
        end

        # Raise fatal exception
        WorkflowMgr.stderr(e.message, 1)
        WorkflowMgr.log(e.message)
        if e.is_a?(ArgumentError) || e.is_a?(NameError) || e.is_a?(TypeError)
          WorkflowMgr.stderr(e.backtrace.join("\n"), 1)
          WorkflowMgr.log(e.backtrace.join("\n"))
        end
        raise "Could not launch batch queue server process."
      end
    end
  end
end
