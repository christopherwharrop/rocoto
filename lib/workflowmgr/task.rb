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

    require 'parsedate'

    attr_reader :seq,:attributes,:envars,:dependency,:deadline,:hangdependency

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

      # Make sure maxtime is not greater than the deadline
      unless @attributes[:deadline].nil?
        deadline=Time.gm(*ParseDate::parsedate(@attributes[:deadline]))
        maxtime=deadline if deadline.to_i < maxtime.getgm.to_i
      end

      # Cap the walltime request to the expiration time of the cycle, or the task deadline, whichever is sooner
      if WorkflowMgr.ddhhmmss_to_seconds(@attributes[:walltime]) + Time.now.getgm.to_i > maxtime.getgm.to_i
        @attributes[:walltime]=WorkflowMgr.seconds_to_hhmmss(maxtime.getgm.to_i - Time.now.to_i)
      end

    end


    #####################################################
    #
    # expired?
    #
    #####################################################
    def expired?(cycle)

      if @attributes[:deadline].nil?
        return false
      else
        return Time.gm(*ParseDate::parsedate(@attributes[:deadline].to_s(cycle.getgm))) <= Time.now.getgm
      end

    end

  end

end
