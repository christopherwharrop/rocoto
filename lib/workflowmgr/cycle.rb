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

    attr_reader :cycle
    attr_reader :activated
    attr_reader :expired
    attr_reader :done
    attr_reader :state

    ##########################################
    #
    # init
    #
    ##########################################
    def initialize(cycle,params={ :activated=>Time.at(0), :expired=>Time.at(0), :done=>Time.at(0) })

      @cycle=cycle
      @activated=params[:activated]
      @expired=params[:expired]
      @done=params[:done]

      if @done != Time.at(0)
        @state=:done
      elsif @expired != Time.at(0)
        @state=:expired
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
      @cycle <=> other.cycle
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

    end  # active?


    ##########################################
    #
    # done?
    #
    ##########################################
    def done?

      return @state==:done

    end  # active?


    ##########################################
    #
    # activate!
    #
    ##########################################
    def activate!

      return if @state==:active
      raise "Expired cycle cannot be activated!" if @state==:expired
      raise "Done cycle cannot be activated!" if @state==:done
      @activated=Time.now.getgm
      @state=:active

    end  # activate!


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

  end  # Class Cycle

end  # Module WorkflowMgr