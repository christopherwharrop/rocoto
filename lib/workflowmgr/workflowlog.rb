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
        @workflowIOServer.log(logname,msg)
      end

    end

  end  # class WorkflowLog

end  # module WorkflowMgr
