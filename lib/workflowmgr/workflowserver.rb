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

    # Get the base directory of the WFM installation
    if File.symlink?(__FILE__)
      __WFMDIR__=File.dirname(File.dirname(File.dirname(File.expand_path(File.readlink(__FILE__),File.dirname(__FILE__)))))
    else
      __WFMDIR__=File.dirname(File.dirname(File.expand_path(File.dirname(__FILE__))))
    end

    $:.unshift("#{__WFMDIR__}/SystemTimer-1.2.3/lib")
    $:.unshift("#{__WFMDIR__}/SystemTimer-1.2.3/ext/system_timer")

    require 'drb'
    require 'system_timer'

    require 'workflowmgr/workflowlog'
    require 'workflowmgr/workflowdb'
    require 'workflowmgr/workflowio'
    require 'workflowmgr/bqsproxy'
    require 'workflowmgr/sgebatchsystem'
    require 'workflowmgr/compoundtimestring'
    require 'workflowmgr/dependency'

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
        raise Timeout::Error,"ERROR: #{@server.class} server is unresponsive"
      end

    end

  end  # class WorkflowServer

end  # module WorkflowMgr


