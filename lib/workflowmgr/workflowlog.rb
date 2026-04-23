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
    def initialize(path, verbosity, workflow_ioserver)
      @path = path
      @verbosity = verbosity || 0
      @workflow_ioserver = workflow_ioserver
    end

    #####################################################
    #
    # log
    #
    #####################################################
    def log(cycle, msg, level = 0)
      if level <= @verbosity
        logname = @path.to_s(cycle)
        begin
          @workflow_ioserver.log(logname, msg)
        rescue WorkflowIOHang
          err = "WARNING! Cannot write the following log message to #{logname} " \
                "because it resides on an unresponsive file system!"
          WorkflowMgr.stderr(err, 2)
          WorkflowMgr.stderr("#{Socket.gethostname} :: #{msg}", 2)
        end
      end
    end
  end
end
