##########################################
#
# module WorkflowMgr
#
##########################################
module WorkflowMgr

  ##########################################
  #
  # Class FileStatProxy
  #
  ##########################################
  class FileStatProxy

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

      # Initialize the filestat server
      initfilestat

      # Define methods
      (WorkflowIO.instance_methods-Object.instance_methods+[:stop!]).each do |m|

        (class << self; self; end).instance_eval do
          define_method m do |*args|
            retries=0
            begin
              SystemTimer.timeout(60) do       
                @fileStatServer.send(m,*args)
              end
            rescue DRb::DRbConnError
              if retries < 1
                retries+=1
                puts "*** WARNING! *** FileStat server process died.  Attempting to restart and try again."
                initfilestat
                retry
              else
                raise "*** ERROR! *** FileStat server process died.  #{retries} attempts to restart the server have failed, giving up."
              end
            rescue Timeout::Error
              raise "*** ERROR! *** FileStat server process is unresponsive and is probably wedged."
            end

          end
        end
      end

    end


    ##########################################
    #
    # respond_to?
    #
    ##########################################
    def respond_to?(name, priv=false)

      return @fileStatServer.respond_to?(name,priv)

    end


    ##########################################
    #
    # method_missing
    #
    ##########################################
    def method_missing(name,*args,&block)
puts "didn't find #{name}"
      retries=0
      begin
        SystemTimer.timeout(60) do       
          return @fileStatServer.send(name,*args,&block)
        end
      rescue DRb::DRbConnError
        if retries < 1
          retries+=1
          puts "*** WARNING! *** FileStat server process died.  Attempting to restart and try again."
          initfilestat
          retry
        else
          raise "*** ERROR! *** FileStat server process died.  #{retries} attempts to restart the server have failed, giving up."
	end
      rescue Timeout::Error
        raise "*** ERROR! *** FileStat server process is unresponsive and is probably wedged."
      end

    end

  private

    ##########################################
    #
    # initfilestat
    #
    ##########################################
    def initfilestat

      # Get the WFM install directory
      wfmdir=File.dirname(File.dirname(File.expand_path(File.dirname(__FILE__))))

      begin

        workflowIO=WorkflowIO.new

        # Set up an object to serve requests for batch queue system services
        if @config.FileStatServer
          @fileStatServer=WorkflowMgr.launchServer("#{wfmdir}/sbin/workflowfilestatserver")
          @fileStatServer.setup(workflowIO)
        else
          @fileStatServer=workflowIO
        end

      rescue

        # Print out the exception message
        puts $!

        # Try to stop the log server if something went wrong
        if @config.FileStatServer
          @fileStatServer.stop! unless @fileStatServer.nil?
        end

      end

    end

  end  # Class FileStatProxy

end  # Module WorkflowMgr
