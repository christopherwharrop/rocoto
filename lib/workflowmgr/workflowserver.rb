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
      return @server.send(name,*args)

    end

  end  # class WorkflowServer

end  # module WorkflowMgr


