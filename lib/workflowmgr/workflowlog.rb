##########################################
#
# Module WorkflowMgr
#
##########################################
module WorkflowMgr

  ########################################## 
  #
  # WorkflowLog
  #
  ##########################################
  class WorkflowLog

    require 'socket'
    require 'workflowmgr/compoundtimestring'
    require 'workflowmgr/utilities'

    #####################################################
    #
    # initialize
    #
    #####################################################
    def initialize(path,verbosity,workflowIOServer)

      @path=path
      @verbosity=verbosity || 0
      @workflowIOServer=workflowIOServer

    end


    #####################################################
    #
    # log
    #
    #####################################################
    def log(cycle,msg,level=0)

      if level <= @verbosity
        logname=@path.to_s(cycle)
        begin
          @workflowIOServer.log(logname,msg)
        rescue WorkflowIOHang
          err="WARNING! Cannot write the following log message to #{logname} because it resides on an unresponsive file system!"
          WorkflowMgr.stderr(err,0)
          WorkflowMgr.stderr("#{Socket.gethostname} :: #{msg}",0)          
        end
      end

    end

  end  # class WorkflowLog

end  # module WorkflowMgr
