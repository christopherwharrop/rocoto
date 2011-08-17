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

    ##########################################
    #
    # initialize
    #
    ##########################################
    def initialize(args)

      @options=WorkflowOption.new(args)
      @config=WorkflowYAMLConfig.new
      @workflowdoc=WorkflowXMLDoc.new(@options.workflowdoc)
      @workflowdb=WorkflowSQLite3DB.new(@options.database)

    end  # initialize


    ##########################################
    #
    # run
    #
    ##########################################
    def run

      begin
        @workflowdb.lock_workflow
      ensure
        @workflowdb.unlock_workflow
      end
 
    end  # run

  end  # Class WorkflowEngine

end  # Module WorkflowMgr


