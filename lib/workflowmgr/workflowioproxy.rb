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

      # Initialize a list of newly detected down file paths
      @newdownpaths=[]

      # Initialize the workflowIO server
      workflowIO_init

      # Define the stop! method to increase performance by avoiding calls to method_missing
      (class << self; self; end).instance_eval do
        define_method :stop! do |*args|
          begin
            SystemTimer.timeout(30) do
              @workflowIOServer.send(:stop!,*args)
            end
          rescue DRb::DRbConnError
            puts "*** ERROR! *** WorkflowIO server process died."
          rescue Timeout::Error
            puts "*** ERROR! *** WorkflowIO server process is unresponsive and is probably wedged."
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
                  raise WorkflowIOHang, "!!! WARNING !!! Cannot attempt to access #{args[0]}, filesystem unresponsive"
                end
              end
            end

            # Check args for paths matching @downpaths
            unless @downpaths.empty?
              @downpaths.each do |downpath|
                if downpath[:path]==args[0][0,downpath[:path].length]

                  # Attempt to kill the process that previously hung
                  system("ssh #{downpath[:host]} kill -9 #{downpath[:pid]} 2>&1 > /dev/null")                  

                  # Check to see if the process that previously hung is still alive
                  system("ssh #{downpath[:host]} kill -0 #{downpath[:pid]} 2>&1 > /dev/null")        
                  if $?.exitstatus==0

                    # The process is still hung, so don't try to access the path because it's still bad.  Raise exception.
                    raise WorkflowIOHang, "!!! WARNING !!! Cannot attempt to access #{args[0]}, filesystem unresponsive"

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
                system("ssh #{downpathmatch[:host]} kill -9 #{downpathmatch[:pid]} 2>&1 > /dev/null")               

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
              workflowIO_init

              raise WorkflowIOHang, "*** ERROR! *** WorkflowIO server process is unresponsive and is probably wedged."

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
          @workflowIOServer,@workflowIOHost,@workflowIOPID=WorkflowMgr.launchServer("#{wfmdir}/sbin/workflowioserver")
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
