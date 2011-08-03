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

      @config=WorkflowYAMLConfig.new
      @options=WorkflowOption.new(args)
      @workflowdoc=WorkflowXMLDoc.new(@options.workflowdoc)
      @workflowdb=WorkflowSQLite3DB.new(@options.database)

    end  # initialize

    ##########################################
    #
    # run
    #
    ##########################################
    def run

      pids=[]    
      1.times do
        pids << Process.fork do
          100.times do
            @workflowdb.test
          end
        end
      end
      pids.each { |pid| Process.waitpid(pid) }

    end  # run

  end  # Class WorkflowEngine

end  # Module WorkflowMgr


