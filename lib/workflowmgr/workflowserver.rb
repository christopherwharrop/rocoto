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

    require 'drb'
    require 'system_timer'

    require 'workflowmgr/workflowlog'
    require 'workflowmgr/workflowdb'
    require 'workflowmgr/workflowio'
    require 'workflowmgr/bqsproxy'
    require 'workflowmgr/sgebatchsystem'
    require 'workflowmgr/moabtorquebatchsystem'
    require 'workflowmgr/torquebatchsystem'
    require 'workflowmgr/lsfbatchsystem'
    require 'workflowmgr/compoundtimestring'
    require 'workflowmgr/dependency'
    require 'workflowmgr/utilities'

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
    # respond_to?
    # 
    ##########################################
    def respond_to?(name, priv=false)

      if @setup
        return @server.respond_to?(name,priv)
      else
        super
      end

    end

    ##########################################
    #
    # method_missing
    # 
    ##########################################
    def method_missing(name,*args,&block)

      raise "Server is not initialized, must call WorkflowServer.setup to initialize it." unless @setup
      begin
        SystemTimer.timeout(60) do
          return @server.send(name,*args,&block)
        end
      rescue Timeout::Error
        WorkflowMgr.log("#{@server.class} server is unresponsive")        
        raise Timeout::Error,"#{@server.class} server is unresponsive"
      end

    end

  end  # class WorkflowServer

end  # module WorkflowMgr


