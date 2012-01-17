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

    require 'fileutils'
    require 'socket'
    require 'workflowmgr/compoundtimestring'

    #####################################################
    #
    # initialize
    #
    #####################################################
    def initialize(path,verbosity)

      @path=path
      @verbosity=verbosity || 0

    end


    #####################################################
    #
    # log
    #
    #####################################################
    def log(cycle,msg,level=0)

      if level <= @verbosity

        logname=@path.to_s(cycle)
        host=Socket.gethostname
        FileUtils.mkdir_p(File.dirname(logname))
        File.open(logname,"a+") do |logfile|
          logfile.puts("#{Time.now} :: #{host} :: #{msg}")
        end

      end

    end

  end  # class WorkflowLog

end  # module WorkflowMgr
