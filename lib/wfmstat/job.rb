##########################################
#
# Module WFMStat
#
##########################################
module WFMStat

  ##########################################
  #
  # Class Job
  #
  ##########################################
  class Job

    include Enumerable

    @@sort_order = [:time, :taskname]

    # pass in an Array of Symbols
    def self.sort_order(neworder)
      @@sort_order = neworder
    end

    attr_reader :taskname, :time, :state, :jobid, :exit_status, :tries

    def initialize(taskname,time,jobid,state,exit_status,tries)
      @taskname = taskname   # String
      @time = time
      @jobid = jobid
      @state = state
      @exit_status = exit_status
      @tries = tries
    end

    # sort by @@sort_order
    def <=>(other)
      # generalize later
      if (@@sort_order.first == :taskname) then
        ret = @taskname <=> other.taskname
        if (@taskname == other.taskname) then
          ret = @time <=> other.time
        end
      else
        ret = @time <=> other.time
        if (@time == other.time) then
          ret = @taskname <=> other.taskname
        end
      end
      ret
    end

  end  # Job

end  # Module WorkflowMgr
