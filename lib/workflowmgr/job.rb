##########################################
#
# Module WorkflowMgr
#
##########################################
module WorkflowMgr

  ##########################################
  #
  # Class Job
  #
  ##########################################
  class Job

    attr_reader   :task,:cycle,:cores
    attr_accessor :id,:state,:native_state,:exit_status,:tries,:nunknowns

    #####################################################
    #
    # initialize
    #
    #####################################################
    def initialize(id,task,cycle,cores,state,native_state,exit_status,tries,nunknowns)

      @id=id
      @task=task
      @cycle=cycle
      @cores=cores
      @state=state
      @native_state=native_state
      @exit_status=exit_status
      @tries=tries
      @nunknowns=nunknowns

    end


    #####################################################
    #
    # pending_submit?
    #
    #####################################################
    def pending_submit?

      @id=~/^druby:/

    end

  end

end
