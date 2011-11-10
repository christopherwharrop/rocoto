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

    #####################################################
    #
    # initialize
    #
    #####################################################
    def initialize(str_objects)

      @str_objects=str_objects.collect do |str|
        if str.is_a?(String)
          str
        else
          if str.has_key?(:cyclestr)
            offset=WorkflowMgr.ddhhmmss_to_seconds(str[:offset])
            cyclestr=str[:cyclestr].gsub(/@(\^?[^@\s])/,'%\1').gsub(/@@/,'@')
            CycleString.new(cyclestr,offset)
          else
            raise "Invalid compound time string element: #{str.inspect}"
          end
        end
      end

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

