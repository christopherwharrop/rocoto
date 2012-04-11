##########################################
#
# Module WorkflowMgr
#
##########################################
module WorkflowMgr


  ##########################################  
  #
  # Class CompoundTimeString
  #
  ##########################################
  class CompoundTimeString

    require 'workflowmgr/cyclestring'
    require 'workflowmgr/utilities'

    #####################################################
    #
    # initialize
    #
    #####################################################
    def initialize(str_objects)

      @str_objects=str_objects

    end


    #####################################################
    #
    # to_s
    #
    #####################################################
    def to_s(time)

      @str_objects.collect {|obj|
        if obj.is_a?(String)
          obj
        else 
          obj.to_s(time.gmtime)
        end
      }.join

    end

  end

end

