##########################################
#
# Module WorkflowMgr
#
##########################################
module WorkflowMgr

  ##########################################  
  #
  # Class WorkflowRewindOption
  #
  ##########################################
  class WorkflowRewindOption < WorkflowOption
    require 'workflowmgr/workflowoption'

    ##########################################  
    #
    # Initialize
    #
    ##########################################
    def initialize(args)
      @cycles=nil
      @tasks=nil
      @all_tasks=nil
      @all_cycles=nil
      super(args)
    end

    def dump()
      puts "Cycles: #{@cycles}"
      puts "Tasks: #{@tasks}"
      puts "Flags: all_tasks=#{@all_tasks.inspect} all_cycles=#{@all_cycles.inspect}"
    end

    ##########################################  
    #
    # cycle accessors
    #
    ##########################################
    def all_cycles?
      if @all_cycles.nil?
        return false
      else
        return @all_cycles[0]
      end
    end
    def each_cycle
      @cycles.each do |cycle|
        yield cycle
      end
    end
    def cycles
      if @cycles.nil?
        fail "@cycles is nil (2)"
      elsif @cycles.empty?
        fail "@cycles is empty (2)"
      end

      cyc=@cycles.clone()
      if cyc.nil?
        fail "cyc is nil"
      elsif cyc.empty?
        fail "cyc is empty"
      end

      return cyc
    end
    def cycles?
      return !@cycles.empty?
    end

    ##########################################  
    #
    # task accessors
    #
    ##########################################
    def all_tasks?
      if @all_tasks.nil?
        return false
      else
        return @all_tasks[0]
      end
    end
    def each_task
      @tasks.each do |task|
        yield task
      end
    end
    def tasks
      return @tasks.clone()
    end
    def tasks?
      return !@tasks.empty?
    end

  private

    ##########################################
    #
    # add_opts
    #
    ##########################################
    def add_opts(opts)
      super(opts)

      # Override the command usage text
      opts.banner = "Usage:  rocotorewind [-h] [-D] [-v #] -d database_file -w workflow_document -c cycle [-c cycle [...]] ( -a | -t task [-t task [...]] )"

      @cycles=Array.new
      @tasks=Array.new
      @all_tasks=Array.new
      @all_tasks.push(false)

      # Specify the cycle
      opts.on("-c","--cycle CYCLE",String,"Cycle") do |c|
        case c
        when /^\d{12}$/
          @cycles.push(Time.gm(c[0..3],c[4..5],c[6..7],c[8..9],c[10..11]))
        else
          puts opts
          puts "Unrecognized -c option #{c}"
          Process.exit
        end
      end

      # Tasks of interest
      opts.on("-t","--task TASK",String,"Task") do |taskstr|
        @tasks.push(taskstr.split(','))
      end

      # Rewind all tasks for the specified cycles instead of a list of tasks:
      opts.on("-a",'--all',"Selects all tasks.") do |flag|
        puts "Requesting rewind of all tasks."
        @all_tasks[0]=true
      end
    end

    ##########################################
    #
    # validate_args
    #
    ##########################################
    def validate_opts(opts,args)
      super(opts,args)
      raise OptionParser::ParseError,"A cycle must be specified." unless cycles?

      if tasks? and @all_tasks[0]
        raise OptionParser::ParseError,"You cannot specify a task list (-t) AND request all tasks (-a).  Give one or the other."
      elsif not tasks and not @all_tasks
        raise OptionParser::ParseError,"A task must be specified."
      end

      if not cycles?
        raise OptionParser::ParseError,"A cycle must be specified."
      end

      if @cycles.nil?
        fail "@cycles is nil"
      elsif @cycles.empty?
        fail "@cycles is empty"
      end
    end
  end
end
