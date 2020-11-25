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
    require 'workflowmgr/utilities'
    require 'drb'

    ##########################################
    #
    # initialize
    #
    ##########################################
    def initialize(dbServer,config,options)

      # Store the log creation parameters
      @dbServer=dbServer
      @config=config
      @options=options

      # Get the list of down file paths from the database
      @downpaths=@dbServer.get_downpaths

      # Initialize a list of newly detected down file paths
      @newdownpaths=[]

      # Initialize the workflowIO server
      workflowIO_init

      # Define the stop! method to increase performance by avoiding calls to method_missing
      (class << self; self; end).instance_eval do
        define_method :stop! do |*args|
          begin
            WorkflowMgr.timeout(30) do
              @workflowIOServer.send(:stop!,*args)
            end
          rescue DRb::DRbConnError
            msg="WARNING! Can't shut down rocotoioserver process #{@workflowIOPID} on host #{@workflowIOHost} because it is not running."
            WorkflowMgr.stderr(msg,2)
            WorkflowMgr.log(msg)
          rescue Timeout::Error
            msg="WARNING! Can't shut down rocotoioserver process #{@workflowIOPID} on host #{@workflowIOHost} because it is unresponsive and is probably wedged."
            WorkflowMgr.stderr(msg,2)
            WorkflowMgr.log(msg)
          end
        end
      end

      # Define other methods on the fly to increase performance by avoiding calls to method_missing
      (WorkflowIO.instance_methods-Object.instance_methods).each do |m|

        (class << self; self; end).instance_eval do
          define_method m do |*args|

            # Check args for paths matching @newdownpaths
            unless @newdownpaths.empty?
              @newdownpaths.each do |downpath|
                if downpath[:path]==args[0][0,downpath[:path].length]
                  # Don't try to access the path because we just detected that accesses to it hang.  Raise exception.
                  raise WorkflowIOHang, "WARNING! rocotoioserver process #{@workflowIOPID} on host #{@workflowIOHost} cannot attempt to access #{args[0]}, because previous attempts to access the filesystem have hung."
                end
              end
            end

            # Check args for paths matching @downpaths
            unless @downpaths.empty?
              @downpaths.each do |downpath|
                if downpath[:path]==args[0][0,downpath[:path].length]

                  # Attempt to kill the process that previously hung
                  system("ssh -o StrictHostKeyChecking=no #{downpath[:host]} kill -9 #{downpath[:pid]} 2>&1 > /dev/null")

                  # Check to see if the process that previously hung is still alive
                  system("ssh -o StrictHostKeyChecking=no #{downpath[:host]} kill -0 #{downpath[:pid]} 2>&1 > /dev/null")
                  if $?.exitstatus==0

                    # The process is still hung, so don't try to access the path because it's still bad.  Raise exception.
                    raise WorkflowIOHang, "WARNING! rocotoioserver process #{@workflowIOPID} on host #{@workflowIOHost} cannot attempt to access #{args[0]}, because previous attempts to access the filesystem have hung."

                  else

                    # The process is gone, so we can attempt to access the bad path again
                    # Remove the bad path from the list of bad paths
                    @dbServer.delete_downpaths([downpath])
                    @downpaths.delete(downpath)

                    # Stop looking for downpaths that match args, we found it
                    break

                  end

                end  # if downpath

              end  # @downpaths.each

            end  # unless @downpaths.empty?

            retries=0
            begin
              WorkflowMgr.timeout(150) do
                @workflowIOServer.send(m,*args)
              end
            rescue DRb::DRbConnError
              if retries < 1
                retries+=1
                msg="WARNING! The rocotoioserver process #{@workflowIOPID} on host #{@workflowIOHost} died.  Attempting to restart and try again."
                WorkflowMgr.stderr(msg,2)
                WorkflowMgr.log(msg)
                workflowIO_init
                retry
              else
                msg="WARNING! The rocotoioserver process #{@workflowIOPID} on host #{@workflowIOHost} died.  #{retries} attempts to restart the server have failed, giving up."
                raise msg
              end
            rescue Timeout::Error

              # The access to the filesystem hung, so record the path in the db to prevent future attempts

              # Find the shortest part of the path in common with known bad paths
              argpath=args[0].split("/")[0..-2]
              commonpath=[]
              downtime=Time.now
              downpathmatch=nil
              (@downpaths+@newdownpaths).each do |path|
                downpath=path[:path].split("/")
                argpath.each_with_index { |p,i| commonpath << p if argpath[i]==downpath[i] }
                downtime=path[:downtime]
                if commonpath.size > 3
                  downpathmatch=path
                  break
                end
              end # @downpaths.each

              # If we found a known down path that matches the current arg path
              if commonpath.size > 3

                # Remove the known down path from the database
                @dbServer.delete_downpaths([downpathmatch])
                @downpaths.delete(downpathmatch)
                @newdownpaths.delete(downpathmatch)

                # Send a kill signal to process associated with the known down path from the database
                # The kill may not work immediately, but hopefully it will remain pending and will be
                # processed once the filesystem comes back to life
                system("ssh -o StrictHostKeyChecking=no #{downpathmatch[:host]} kill -9 #{downpathmatch[:pid]} 2>&1 > /dev/null")

                # Add the common portion of the paths to the database
                newdownpath={:path=>commonpath.join("/"), :downtime=>downtime, :host=>@workflowIOHost, :pid=>@workflowIOPID }
                @dbServer.add_downpaths([newdownpath])
                @newdownpaths << newdownpath

              # Otherwise the arg path is a new down path
              else

                newdownpath={:path=>argpath.join("/"), :downtime=>downtime, :host=>@workflowIOHost, :pid=>@workflowIOPID }
                @dbServer.add_downpaths([newdownpath])
                @newdownpaths << newdownpath

              end  # if commonpath.size

              # Restart the workflowIO server
              msg="WARNING! The rocotoioserver process #{@workflowIOPID} on host #{@workflowIOHost} is unresponsive while accessing #{args[0]} and is probably wedged."
              workflowIO_init
              raise WorkflowIOHang, msg

            end  # begin

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

          # Ignore SIGINT while launching server process
          Signal.trap("INT",nil)

          @workflowIOServer,@workflowIOHost,@workflowIOPID=WorkflowMgr.launchServer("#{wfmdir}/sbin/rocotoioserver")
          @workflowIOServer.setup(workflowIO)

          # Restore default SIGINT handler
          Signal.trap("INT","DEFAULT")

        else
          @workflowIOServer=workflowIO
        end

      rescue => crash

        # Try to stop the log server if something went wrong
        if @config.WorkflowIOServer
          @workflowIOServer.stop! unless @workflowIOServer.nil?
        end

        # Raise fatal exception
        WorkflowMgr.stderr(crash.message,1)
        WorkflowMgr.log(crash.message)
        case
          when crash.is_a?(ArgumentError),crash.is_a?(NameError),crash.is_a?(TypeError)
            WorkflowMgr.stderr(crash.backtrace.join("\n"),1)
            WorkflowMgr.log(crash.backtrace.join("\n"))
          else
        end
        raise "Could not launch IO server process."

      end

    end

  end  # Class WorkflowIOProxy

end  # Module WorkflowMgr
