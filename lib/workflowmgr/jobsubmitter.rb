##########################################
#
# Module WorkflowMgr
#
##########################################
module WorkflowMgr

  ##########################################
  #
  # Class JobSubmitter
  #
  ##########################################
  class JobSubmitter
  
    require 'drb'

    ##########################################
    #
    # initialize
    #
    ##########################################
    def initialize(scheduler)
      @scheduler=scheduler
      @jobid=nil
      @output=nil
    end

    ##########################################
    #
    # submit
    #
    ##########################################
    def submit(command,options)
      Thread.new { 
        @jobid,@output=@scheduler.submit(command,options)
      }
    end

    ##########################################
    #
    # getjobid
    #
    ##########################################
    def getjobid
      return @jobid
    end

    ##########################################
    #
    # getoutput
    #
    ##########################################
    def getoutput
      return @output
    end

    ##########################################
    #
    # stop!
    #
    ##########################################
    def stop!
      DRb.stop_service
    end

  end

end
