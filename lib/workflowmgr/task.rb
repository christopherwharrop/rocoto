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
      @rewind_list=[]

      @native_list=[]

      # Set a default value for maxtries
      @attributes[:maxtries]=9999999 if @attributes[:maxtries].nil?

      # Set a default value for throttle
      @attributes[:throttle]=9999999 if @attributes[:throttle].nil?

      # Set a default value for final
      @attributes[:final]=false if @attributes[:final].nil?

    end

    #####################################################
    #
    # native job card line functionality
    #
    #####################################################
    def add_native(native)
      @native_list.push(native)
    end
    def each_native()
      @native_list.each do |x|
        yield x
      end
    end
    def natives?()
      return ! @native.empty?
    end

    #####################################################
    #
    # rewind functionality
    #
    #####################################################
    def add_rewind_action(rewinder)
      @rewind_list.push(rewinder)
    end
    def rewind!(wstate)
      @rewind_list.each do |rewinder|
        rewinder.rewind!(wstate)
      end
    end
    def each_rewind_action()
      @rewind_list.each do |rewinder|
        yield rewinder
      end
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

      t=Task.new(@seq,attributes,envars,@dependency,@hangdependency)

      each_rewind_action do |rewinder|
        t.add_rewind_action(rewinder)
      end

      natives=[]
      each_native do |native|
        if native.is_a?(CompoundTimeString)
          t.add_native(native.to_s(cycle))
        else
          t.add_native(native)
        end
      end

      return t
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
