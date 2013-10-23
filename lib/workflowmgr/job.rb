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
    attr_accessor :id,:state,:native_state,:exit_status,:tries,:nunknowns,:duration

    #####################################################
    #
    # initialize
    #
    #####################################################
    def initialize(id,task,cycle,cores,state,native_state,exit_status,tries,nunknowns,duration)

      @id=id
      @task=task
      @cycle=cycle
      @cores=cores
      @state=state
      @native_state=native_state
      @exit_status=exit_status
      @tries=tries
      @nunknowns=nunknowns
      @duration=duration

    end


    #####################################################
    #
    # pending_submit?
    #
    #####################################################
    def pending_submit?

      @id=~/^druby:/

    end


    #####################################################
    #
    # done?
    #
    #####################################################
    def done?

      @state == "SUCCEEDED" || @state == "FAILED" || @state == "DEAD" || @state == "LOST"

    end


    #####################################################
    #
    # failed?
    #
    #####################################################
    def failed?

      @state == "FAILED" || @state == "DEAD" || @state == "LOST"

    end


    #####################################################
    #
    # dead?
    #
    #####################################################
    def dead?

      @state == "DEAD"

    end


    #####################################################
    #
    # expired?
    #
    #####################################################
    def expired?

      @state == "EXPIRED"

    end

  end

end
