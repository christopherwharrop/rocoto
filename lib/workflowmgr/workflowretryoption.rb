##########################################
#
# Module WorkflowMgr
#
##########################################
module WorkflowMgr

  ##########################################  
  #
  # Class WorkflowRetryOption
  #
  ##########################################
  class WorkflowRetryOption < WorkflowOption
    require 'workflowmgr/workflowoption'

    ##########################################  
    #
    # Initialize
    #
    ##########################################
    def initialize(args)
      @cycles=nil
      @tasks=nil
      @all_tasks=false
      @all_cycles=false  # always false; no dash option exists
      super(args)
    end

    def dump()
      puts "Cycles: #{@cycles}"
      puts "Tasks: #{@tasks}"
      puts "Flags: all_tasks=#{@all_tasks} all_cycles=#{@all_cycles}"
    end

    ##########################################  
    #
    # cycle accessors
    #
    ##########################################
    def all_cycles?
      return @all_cycles
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
      return @all_tasks
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
      opts.banner = "Usage:  rocotoretry [-h] [-D] [-v #] -d database_file -w workflow_document -c cycle [-c cycle [...]] ( -a | -t task [-t task [...]] )"

      @cycles=Array.new
      @tasks=Array.new

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

      # Retry all tasks for the specified cycles instead of a list of tasks:
      opts.on("-a",'--all',"Selects all tasks.") do |flag|
        @all_tasks=flag
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

      if tasks? and @all_tasks
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
      else
        puts "In validate, @cycles is <#{@cycles}>"
      end
    end
  end
end
