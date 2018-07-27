##########################################
#
# Module WorkflowMgr
#
##########################################
module WorkflowMgr

  ALL_POSSIBLE_CYCLES=(Time.gm(1900,1,1,0,0)..Time.gm(9999,12,31,23,59))

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

  ##########################################  
  #
  # Class TaskSelection
  #
  ##########################################
  class TaskSelection
    # Stores the contents of a -t option from argument parsing
    attr_reader :arg
    def initialize(arg) @arg=arg ; end
  end # class TaskSelection

  ##########################################  
  #
  # Class MetataskSelection
  #
  ##########################################
  class MetataskSelection
    # Stores the contents of a -m option from argument parsing
    attr_reader :arg
    def initialize(arg) @arg=arg ; end
  end # class MetataskSelection



end # module WorkflowMgr
