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

    attr :str_objects

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
    def to_s(time=nil)

      @str_objects.collect {|obj|
        if obj.is_a?(String)
          obj
        else 
          obj.to_s(time)
        end
      }.join.strip

    end


    #####################################################
    #
    # to_s
    #
    #####################################################
    def inspect()

      @str_objects.collect {|obj|
        if obj.is_a?(String)
          obj
        else 
          obj.inspect()
        end
      }.join.strip

    end


    #####################################################
    #
    # hash
    #
    #####################################################
    def hash

      @str_objects.hash

    end 


    #####################################################
    #
    # eql?
    #
    #####################################################
    def eql?(other)

      return @str_objects==other.str_objects

    end

  end


end

