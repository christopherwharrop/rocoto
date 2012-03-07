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
    def initialize(path,verbosity,fileStatServer)

      @path=path
      @verbosity=verbosity || 0
      @fileStatServer=fileStatServer

    end


    #####################################################
    #
    # log
    #
    #####################################################
    def log(cycle,msg,level=0)

      if level <= @verbosity
        logname=@path.to_s(cycle)
        @fileStatServer.log(logname,msg)
      end

    end

  end  # class WorkflowLog

end  # module WorkflowMgr
