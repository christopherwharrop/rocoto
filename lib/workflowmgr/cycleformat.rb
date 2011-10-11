##########################################
#
# Module WorkflowMgr
#
##########################################
module WorkflowMgr

  ########################################## 
  #
  # Class CycleFormat 
  #
  ##########################################
  class CycleFormat

    #####################################################
    #
    # initialize
    #
    #####################################################
    def initialize(format,offset)

      @format=format
      @offset=offset

    end

    #####################################################
    #
    # to_s
    #
    #####################################################
    def to_s(cycle)

      return (cycle.gmtime+@offset).strftime(@format)

    end

  end

end
