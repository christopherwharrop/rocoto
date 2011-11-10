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

    #####################################################
    #
    # initialize
    #
    #####################################################
    def initialize(log)

      @path=CompoundTimeString.new(log[:path])
      @verbosity=log[:verbosity] || 0

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
          WorkflowMgr.forkit(1) do
            host=Socket.gethostname
            FileUtils.mkdir_p(File.dirname(logname))
            logfile=File.new(logname,"a+")
            logfile.puts("#{Time.now} :: #{host} :: #{msg}")
            logfile.close
          end  # forkit
        rescue WorkflowMgr::ForkitTimeoutException
          WorkflowMgr.ioerr(logname)
        end  # begin

      end

    end

  end  # class WorkflowLog

end  # module WorkflowMgr
