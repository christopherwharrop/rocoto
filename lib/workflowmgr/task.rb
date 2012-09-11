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

    attr_reader :seq,:attributes,:envars,:dependency,:hangdependency

    #####################################################
    #
    # initialize
    #
    #####################################################
    def initialize(seq,attributes,envars,dependency,hangdependency)

      @seq=seq
      @attributes=attributes
      @envars=envars
      @dependency=dependency
      @hangdependency=hangdependency

      # Set a default value for maxtries
      @attributes[:maxtries]=9999999 if @attributes[:maxtries].nil?

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

      return Task.new(@seq,attributes,envars,@dependency,@hangdependency)

    end

    #####################################################
    #
    # cap_walltime
    #
    #####################################################
    def cap_walltime(maxtime)

      if WorkflowMgr.ddhhmmss_to_seconds(@attributes[:walltime]) + Time.now.getgm.to_i > maxtime.getgm.to_i
        @attributes[:walltime]=WorkflowMgr.seconds_to_hhmmss(maxtime.getgm.to_i - Time.now.to_i)
      end

    end

  end

end
