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

    require 'workflowmgr/workflowlog'
    require 'workflowmgr/workflowdb'
    require 'workflowmgr/workflowio'
    require 'workflowmgr/bqsproxy'
    require 'workflowmgr/moabbatchsystem'
    require 'workflowmgr/moabtorquebatchsystem'
    require 'workflowmgr/torquebatchsystem'
    require 'workflowmgr/pbsprobatchsystem'
    require 'workflowmgr/lsfbatchsystem'
    require 'workflowmgr/lsfcraybatchsystem'
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
        WorkflowMgr.timeout(40) do
          return @server.send(name,*args,&block)
        end
      rescue Timeout::Error
        localhostinfo=Socket::getaddrinfo(Socket.gethostname, nil, nil, Socket::SOCK_STREAM)[0]
        msg="WARNING! #{File.basename($0)} process #{Process.pid} on host #{localhostinfo[2]} (#{localhostinfo[3]}) timed out while calling #{@server.class}.#{name}"
        WorkflowMgr.log(msg)
        WorkflowMgr.stderr(msg,2)
        raise
      end

    end

  end  # class WorkflowServer

end  # module WorkflowMgr


