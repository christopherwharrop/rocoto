##########################################
#
# Module WFMStat
#
##########################################
module WFMStat

  require 'workflowmgr/workflowsubsetoptions'
  require 'workflowmgr/workflowdbselection'

  ##########################################  
  #
  # Class StatusOption
  # 
  ##########################################
  class WFMStatOption < WorkflowMgr::WorkflowSubsetOptions

    require 'optparse'
    require 'pp'                      
    require 'parsedate'
    
    attr_reader :database, :workflowdoc, :cycles, :tasks, :summary, :taskfirst, :verbose

    ##########################################  
    #
    # Initialize
    #
    ##########################################
    def initialize(args,name,action)

      @summary=false
      @taskfirst=false
      super(args,name,action,true)

    end  # initialize

  private

    ##########################################  
    #
    # add_opts
    #
    ##########################################
    def add_opts(opts)

      super(opts)

      # Command usage text
      opts.banner = "Usage:  #{@name} [-h] [-v #] -d database_file -w workflow_document [-c cycle_list] [-t task_list] [-m metatask_list] [-a] [-s] [-T]"
      
      # cycle summary
      opts.on("-s","--summary","Cycle Summary") do 
        @summary=true
      end

    end # add_opts

    def make_selection()
      @selection=WorkflowMgr::WorkflowDBSelection.new(@all_tasks,@tasks,@metatasks,@cycles,@default_all)
      return @selection
    end

  end  # Class StatusOption

end  # Module WFMStat
