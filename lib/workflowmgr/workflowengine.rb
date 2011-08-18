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

      begin

        # Initialize the database
        @workflowdb=WorkflowSQLite3DB.new(@options.database)

        # Acquire a lock on the workflow in the database
        @workflowdb.lock_workflow

        # Initialize the workflow document
        @workflowdoc=WorkflowXMLDoc.new(@options.workflowdoc)

      ensure

        # Make sure we release the workflow lock in the database
        @workflowdb.unlock_workflow

      end
 
    end  # run

  end  # Class WorkflowEngine

end  # Module WorkflowMgr


