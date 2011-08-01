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

    require 'workflowmgr/wfmconfig'
    require 'workflowmgr/wfmoptions'

    ##########################################
    #
    # initialize
    #
    ##########################################
    def initialize(args)

      @config=WFMYAMLConfig.new
      @options=WFMOptions.new(args)

    end  # initialize

    ##########################################
    #
    # run
    #
    ##########################################
    def run


    end  # run

  end  # Class WorkflowEngine

end  # Module WorkflowMgr


