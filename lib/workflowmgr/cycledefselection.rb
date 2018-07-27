##########################################
#
# Module WorkflowMgr
#
##########################################
module WorkflowMgr
  ##########################################  
  #
  # Class CycleDefSelection
  #
  ##########################################
  class CycleDefSelection

    # This class is just a dumb wrapper around a cycledef name.  It is
    # needed to pass a cycledef name as something other than a string,
    # so that the receiver knows that it is a cycledef.

    attr_reader :name
    def initialize(name)
      @name=name
    end
  end # class CycleDefSelection
end # module WorkflowMgr
