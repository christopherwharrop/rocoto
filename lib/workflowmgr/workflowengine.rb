##########################################
#
# Module WorkflowMgr
#
##########################################
module WorkflowMgr

  ##########################################
  #
  # Class WorkflowEngine
  #
  ##########################################
  class WorkflowEngine

    require 'workflowmgr/workflowconfig'
    require 'workflowmgr/workflowoption'
    require 'workflowmgr/workflowdoc'
    require 'workflowmgr/workflowdb'
    require 'workflowmgr/cycle'

    ##########################################
    #
    # initialize
    #
    ##########################################
    def initialize(args)

      # Get command line options
      @options=WorkflowOption.new(args)

      # Get configuration file options
      @config=WorkflowYAMLConfig.new

    end  # initialize


    ##########################################
    #
    # run
    #
    ##########################################
    def run

      # Initialize the database
      @workflowdb=WorkflowSQLite3DB.new(@options.database)

      # Acquire a lock on the workflow in the database
      @workflowdb.lock_workflow

      begin

        # Initialize the workflow document
        @workflowdoc=WorkflowXMLDoc.new(@options.workflowdoc)

        # Make sure the database contains the current cycle specs
        @workflowdb.update_cyclespecs(@workflowdoc.cycles)

        # Get the cycle specs from the database       
        @cyclespecs=@workflowdb.get_cyclespecs.collect do |cyclespec|
          fieldstr=cyclespec[:fieldstr].split(/\s+/)
          if fieldstr.size==3
            CycleInterval.new(cyclespec[:group],fieldstr,cyclespec[:dirty])
          elsif fieldstr.size==6
            CycleCron.new(cyclespec[:group],fieldstr,cyclespec[:dirty])
          else
            raise "ERROR: Unsupported <cycle> type!"
          end
        end



      ensure

        # Make sure we release the workflow lock in the database
        @workflowdb.unlock_workflow

      end
 
    end  # run

  end  # Class WorkflowEngine

end  # Module WorkflowMgr


