##########################################
#
# Module WorkflowMgr
#
##########################################
module WorkflowMgr

  ##########################################
  #
  # WorkflowServer
  #
  ##########################################
  class WorkflowServer

    require 'workflowmgr/workflowlog'
    require 'workflowmgr/workflowdb'
    require 'timeout'

    #####################################################
    #
    # initialize
    #
    #####################################################
    def initialize

      @server=nil
      @setup=false

    end


    #####################################################
    #
    #
    #
    #####################################################
    def setup(serveobj)

      @server=serveobj
      @setup=true

    end


    ##########################################
    #
    # stop!
    #
    ##########################################
    def stop!
      DRb.stop_service
    end


    ##########################################
    #
    # method_missing
    # 
    ##########################################
    def method_missing(name,*args)

      raise "Server is not initialized, must call WorkflowServer.setup to initialize it." unless @setup
      begin
        Timeout.timeout(10) do
          return @server.send(name,*args)
        end
      rescue Timeout::Error
        raise "Server is unresponsive"
      end

    end

  end  # class WorkflowServer

end  # module WorkflowMgr


