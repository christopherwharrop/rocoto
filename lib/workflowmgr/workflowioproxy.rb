##########################################
#
# module WorkflowMgr
#
##########################################
require 'English'
module WorkflowMgr
  ##########################################
  #
  # Class WorkflowIOProxy
  #
  ##########################################
  class WorkflowIOProxy
    require 'workflowmgr/workflowio'
    require 'workflowmgr/utilities'
    require 'drb'

    ##########################################
    #
    # initialize
    #
    ##########################################
    def initialize(db_server, config, options)
      # Store the log creation parameters
      @db_server = db_server
      @config = config
      @options = options

      # Get the list of down file paths from the database
      @downpaths = @db_server.get_downpaths

      # Initialize a list of newly detected down file paths
      @newdownpaths = []

      # Initialize the workflow_io server
      workflow_io_init

      # Define the stop! method to increase performance by avoiding calls to method_missing
      (class << self; self; end).instance_eval do
        define_method :stop! do |*args|
          WorkflowMgr.timeout(30) do
            @workflow_io_server.send(:stop!, *args)
          end
        rescue DRb::DRbConnError
          msg = "WARNING! Can't shut down rocotoioserver process #{@workflow_io_pid} on host #{@workflow_io_host} " \
                "because it is not running."
          WorkflowMgr.stderr(msg, 2)
          WorkflowMgr.log(msg)
        rescue Timeout::Error
          msg = "WARNING! Can't shut down rocotoioserver process #{@workflow_io_pid} on host #{@workflow_io_host} " \
                "because it is unresponsive and is probably wedged."
          WorkflowMgr.stderr(msg, 2)
          WorkflowMgr.log(msg)
        end
      end

      # Define other methods on the fly to increase performance by avoiding calls to method_missing
      (WorkflowIO.instance_methods - Object.instance_methods).each do |m|
        (class << self; self; end).instance_eval do
          define_method m do |*args|
            # Check args for paths matching @newdownpaths
            unless @newdownpaths.empty?
              @newdownpaths.each do |downpath|
                next unless downpath[:path] == args[0][0, downpath[:path].length]

                # Don't try to access the path because we just detected that accesses to it hang.  Raise exception.
                raise WorkflowIOHang,
                      "WARNING! rocotoioserver process #{@workflow_io_pid} on host #{@workflow_io_host} " \
                      "cannot attempt to access #{args[0]}, because previous attempts to " \
                      "access the filesystem have hung."
              end
            end

            # Check args for paths matching @downpaths
            unless @downpaths.empty?
              @downpaths.each do |downpath|
                next unless downpath[:path] == args[0][0, downpath[:path].length]

                # Attempt to kill the process that previously hung
                system("ssh -o StrictHostKeyChecking=no #{downpath[:host]} kill -9 #{downpath[:pid]} 2>&1 > /dev/null")

                # Check to see if the process that previously hung is still alive
                system("ssh -o StrictHostKeyChecking=no #{downpath[:host]} kill -0 #{downpath[:pid]} 2>&1 > /dev/null")
                if $CHILD_STATUS.exitstatus == 0

                  # The process is still hung, so don't try to access the path because it's still bad.  Raise exception.
                  raise WorkflowIOHang,
                        "WARNING! rocotoioserver process #{@workflow_io_pid} on host #{@workflow_io_host} " \
                        "cannot attempt to access #{args[0]}, because previous attempts to " \
                        "access the filesystem have hung."

                else

                  # The process is gone, so we can attempt to access the bad path again
                  # Remove the bad path from the list of bad paths
                  @db_server.delete_downpaths([downpath])
                  @downpaths.delete(downpath)

                  # Stop looking for downpaths that match args, we found it
                  break

                end

                # if downpath
              end

            end

            retries = 0
            begin
              WorkflowMgr.timeout(150) do
                @workflow_io_server.send(m, *args)
              end
            rescue DRb::DRbConnError
              if retries < 1
                retries += 1
                msg = "WARNING! The rocotoioserver process #{@workflow_io_pid} on host #{@workflow_io_host} died.  " \
                      "Attempting to restart and try again."
                WorkflowMgr.stderr(msg, 2)
                WorkflowMgr.log(msg)
                workflow_io_init
                retry
              else
                msg = "WARNING! The rocotoioserver process #{@workflow_io_pid} on host #{@workflow_io_host} died.  " \
                      "#{retries} attempts to restart the server have failed, giving up."
                raise msg
              end
            rescue Timeout::Error
              # The access to the filesystem hung, so record the path in the db to prevent future attempts

              # Find the shortest part of the path in common with known bad paths
              argpath = args[0].split("/")[0..-2]
              commonpath = []
              downtime = Time.now
              downpathmatch = nil
              (@downpaths + @newdownpaths).each do |path|
                downpath = path[:path].split("/")
                argpath.each_with_index { |p, i| commonpath << p if argpath[i] == downpath[i] }
                downtime = path[:downtime]
                if commonpath.size > 3
                  downpathmatch = path
                  break
                end
              end

              # If we found a known down path that matches the current arg path
              if commonpath.size > 3

                # Remove the known down path from the database
                @db_server.delete_downpaths([downpathmatch])
                @downpaths.delete(downpathmatch)
                @newdownpaths.delete(downpathmatch)

                # Send a kill signal to process associated with the known down path from the database
                # The kill may not work immediately, but hopefully it will remain pending and will be
                # processed once the filesystem comes back to life
                system("ssh -o StrictHostKeyChecking=no #{downpathmatch[:host]} kill -9 " \
                       "#{downpathmatch[:pid]} 2>&1 > /dev/null")

                # Add the common portion of the paths to the database
                newdownpath = { path: commonpath.join("/"), downtime: downtime, host: @workflow_io_host,
                                pid: @workflow_io_pid }
                @db_server.add_downpaths([newdownpath])
                @newdownpaths << newdownpath

              # Otherwise the arg path is a new down path
              else

                newdownpath = { path: argpath.join("/"), downtime: downtime, host: @workflow_io_host,
                                pid: @workflow_io_pid }
                @db_server.add_downpaths([newdownpath])
                @newdownpaths << newdownpath

              end

              # Restart the workflow_io server
              msg = "WARNING! The rocotoioserver process #{@workflow_io_pid} on host #{@workflow_io_host} " \
                    "is unresponsive while accessing #{args[0]} and is probably wedged."
              workflow_io_init
              raise WorkflowIOHang, msg
            end
          end
        end
      end
    end


    private

    ##########################################
    #
    # workflow_io_init
    #
    ##########################################
    def workflow_io_init
      # Get the WFM install directory
      wfmdir = File.dirname(File.dirname(__dir__))

      begin
        workflow_io = WorkflowIO.new

        # Set up an object to serve requests for batch queue system services
        # Skip launching daemon in dryrun mode - no I/O operations needed
        if @config.WorkflowIOServer && !WorkflowMgr.dryrun_mode?

          # Ignore SIGINT while launching server process
          Signal.trap("INT", nil)

          @workflow_io_server, @workflow_io_host, @workflow_io_pid = WorkflowMgr.launch_server("#{wfmdir}/sbin/rocotoioserver")
          @workflow_io_server.setup(workflow_io)

          # Restore default SIGINT handler
          Signal.trap("INT", "DEFAULT")

        else
          @workflow_io_server = workflow_io
        end
      rescue StandardError => e
        # Try to stop the log server if something went wrong
        # Only attempt daemon shutdown when a daemon was actually launched (not in dryrun)
        if @config.WorkflowIOServer && !WorkflowMgr.dryrun_mode? &&
           !(@workflow_io_server.nil? || !@workflow_io_server.respond_to?(:stop!))
          @workflow_io_server.stop!
        end

        # Raise fatal exception
        WorkflowMgr.stderr(e.message, 1)
        WorkflowMgr.log(e.message)
        if e.is_a?(ArgumentError) || e.is_a?(NameError) || e.is_a?(TypeError)
          WorkflowMgr.stderr(e.backtrace.join("\n"), 1)
          WorkflowMgr.log(e.backtrace.join("\n"))
        end
        raise "Could not launch IO server process."
      end
    end
  end
end
