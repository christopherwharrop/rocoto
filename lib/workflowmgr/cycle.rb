##########################################
#
# Module WorkflowMgr
#
##########################################
module WorkflowMgr

  ########################################## 
  #
  # Class Cycle
  #
  ##########################################
  class Cycle

    include Comparable

    attr_reader :cycle
    attr_reader :activated
    attr_reader :expired
    attr_reader :draining
    attr_reader :done
    attr_reader :state

    ##########################################
    #
    # init
    #
    ##########################################
    def initialize(cycle,params={ :activated=>Time.at(0), :expired=>Time.at(0), :done=>Time.at(0), :draining=>Time.at(0) })

      @cycle=cycle
      @activated=params[:activated] || Time.at(0)
      @expired=params[:expired] || Time.at(0)
      @draining=params[:draining] || Time.at(0)
      @done=params[:done] || Time.at(0)

      if @done != Time.at(0)
        @state=:done
      elsif @expired != Time.at(0)
        @state=:expired
      elsif @draining != Time.at(0)
        @state=:draining
      elsif @activated != Time.at(0)
        @state=:active
      else
        @state=:inactive
      end

    end

    
    ##########################################
    #
    # <=>
    #
    ##########################################
    def <=>(other)
      @cycle.getgm.to_i <=> other.cycle.getgm.to_i
    end


    ##########################################
    #
    # active?
    #
    ##########################################
    def active?

      return @state==:active

    end  # active?


    ##########################################
    #
    # expired?
    #
    ##########################################
    def expired?

      return @state==:expired

    end  # expired?


    ##########################################
    #
    # draining?
    #
    ##########################################
    def draining?

      return @state==:draining

    end  # draining?


    ##########################################
    #
    # done?
    #
    ##########################################
    def done?

      return @state==:done

    end  # done?


    ##########################################
    #
    # activate!
    #
    ##########################################
    def activate!

      return if @state==:active
      raise "Expired cycle cannot be activated!" if @state==:expired
      raise "Done cycle cannot be activated!  Use reactivate!" if @state==:done
      raise "Draining cycle cannot be activated!" if @state==:draining
      @activated=Time.now.getgm
      @state=:active

    end  # activate!


    ##########################################
    #
    # reactivate!
    #
    ##########################################
    def reactivate!

      return if @state==:active
      raise "Expired cycle cannot be reactivated!" if @state==:expired
      raise "Draining cycle cannot be reactivated!" if @state==:draining

      @done=Time.at(0)
      @state=:active

    end  # activate!


    ##########################################
    #
    # drain!
    #
    ##########################################
    def drain!

      return if @state==:draining
      raise "Done cycle cannot be drained!" if @state==:done
      raise "Expired cycle cannot be drained!" if @state==:expired
      @draining=Time.now.getgm
      @state=:draining

    end  # expire!


    ##########################################
    #
    # expire!
    #
    ##########################################
    def expire!

      return if @state==:expired
      raise "Done cycle cannot be expired!" if @state==:done
      @expired=Time.now.getgm
      @state=:expired

    end  # expire!


    ##########################################
    #
    # done!
    #
    ##########################################
    def done!

      return if @state==:done
      raise "Expired cycle cannot be completed!" if @state==:expired
      @done=Time.now.getgm
      @state=:done

    end  # done!


    ##########################################
    #
    # activated_time_string
    #
    # sets activated time based on state  [mon dd, YYYY HH:MM:SS]
    #
    ##########################################
    def activated_time_string(fmt="%b %d %Y %H:%M:%S")

      case @state
        when :inactive
          activated="-"
        when :active, :done, :expired, :draining
          activated=@activated.strftime(fmt)
      end
      activated
    end

    ##########################################
    #
    # deactivated_time_string
    #
    # sets deactivated time based on state
    #
    ##########################################
    def deactivated_time_string(fmt="%b %d %Y %H:%M:%S")

      case @state
        when :inactive, :active, :draining
          deactivated="-"
        when :done
          deactivated=@done.strftime(fmt)
        when :expired
          deactivated=@expired.strftime(fmt)
      end
      deactivated
    end

  end  # Class Cycle

end  # Module WorkflowMgr
