##########################################
#
# Module WorkflowMgr
#
##########################################
module WorkflowMgr

  ##########################################
  #
  # Class Task
  #
  ##########################################
  class Task

    attr_reader :attributes,:envars,:dependency

    #####################################################
    #
    # initialize
    #
    #####################################################
    def initialize(attributes,envars,dependency)

      @attributes=attributes
      @envars=envars
      @dependency=dependency

    end


    #####################################################
    #
    # localize
    #
    #####################################################
    def localize(cycle)

      attributes={}
      @attributes.each do |attrkey,attrval|
        if attrval.is_a?(CompoundTimeString)
          val=attrval.to_s(cycle)
        else
          val=attrval
        end
        attributes[attrkey]=val
      end

      envars={}
      @envars.each do |envarkey,envarval|
        if envarkey.is_a?(CompoundTimeString)
          key=envarkey.to_s(cycle)
        else
          key=envarkey
        end
        if envarval.is_a?(CompoundTimeString)
          val=envarval.to_s(cycle)
        else
          val=envarval
        end
        envars[key]=val
      end

      return Task.new(attributes,envars,@dependency)

    end



  end

end
