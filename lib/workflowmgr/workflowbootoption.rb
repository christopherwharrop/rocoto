##########################################
#
# Module WorkflowMgr
#
##########################################
module WorkflowMgr

  ##########################################  
  #
  # Class WorkflowBootOption
  #
  ##########################################
  class WorkflowBootOption < WorkflowOption

    ALL_CYCLES=(Time.gm(1900,1,1,0,0)..Time.gm(9999,12,31,23,59))

    require 'workflowmgr/workflowoption'
    require 'workflowmgr/workflowselection'

    attr_reader :database, :workflowdoc, :cycles, :tasks, :metatasks, :verbose, :all_tasks, :selection

    ##########################################  
    #
    # Initialize
    #
    ##########################################
    def initialize(args,name='rocotoboot',action='boot',default_all=false)

      @cycles=nil
      @tasks=nil
      @metatasks=nil
      @default_all=default_all  # true => command defaults to all tasks and cycles
      @name=name
      @action=action
      @all_tasks=false
      @all_cycles=false
      super(args)

      puts("default_all=#{@default_all}")

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
      opts.banner = "Usage:  #{@name} [-h] [-v #] -d database_file -w workflow_document [-c cycle_list] [-t task_list] [-m metatask_list] [-a]"

      # Cycles of interest
      #      C   C,C,C  C:C  :C   C:
      #        where C='YYYYMMDDHHMM', C:  >= C, :C  <= C 
      opts.on("-c","--cycles 'c1,c2,c3' | 'c1:c2' | ':c' | 'c:' | : | all",String,"List of cycles") do |clist|
        @cycles=[] if @cycles.nil?
        case clist
        when /^\d{12}(,\d{12})*$/
          @cycles.concat(clist.split(",").collect { |c| Time.gm(c[0..3],c[4..5],c[6..7],c[8..9],c[10..11]) })
        when /^(\d{12}):(\d{12})$/
          @cycles<< (Time.gm($1[0..3],$1[4..5],$1[6..7],$1[8..9],$1[10..11])..Time.gm($2[0..3],$2[4..5],$2[6..7],$2[8..9],$2[10..11]))
        when /^:(\d{12})$/
          @cycles<< (Time.gm(1900,1,1,0,0)..Time.gm($1[0..3],$1[4..5],$1[6..7],$1[8..9],$1[10..11]))
        when /^(\d{12}):$/
          @cycles<< (Time.gm($1[0..3],$1[4..5],$1[6..7],$1[8..9],$1[10..11])..Time.gm(9999,12,31,23,59))
        when /^all|:$/i
          @cycles<< ALL_CYCLES
          @all_cycles=true
        else
          puts opts
          puts "Unrecognized -c option #{clist}"
          Process.exit
        end
      end

      @cycles=nil if !@cycles.nil? and @cycles.empty?

      # Tasks of interest
      opts.on("-t","--tasks 'a,b,c'",Array,"List of tasks") do |tasklist|
        @tasks=[] if @tasks.nil?
        @tasks.concat tasklist unless tasklist.nil?
      end

      @tasks=nil if !@tasks.nil? and @tasks.empty?

      # Metaasks of interest
      opts.on("-m","--metatasks 'a,b,c'",Array,"List of metatasks") do |metatasklist|
        @metatasks=[] if @metatasks.nil?
        @metatasks.concat metatasklist unless metatasklist.nil?
      end

      @metatasks=nil if !@metatasks.nil? and @metatasks.empty?

      # Rewind all tasks for the specified cycles instead of a list of tasks:
      opts.on("-a",'--all',"Selects all tasks.") do |flag|
        @all_tasks=true
      end

    end

    ##########################################
    #
    # validate_args
    #
    ##########################################
    def validate_opts(opts,args)

      super(opts,args)

      puts("default_all=#{@default_all}")

      if @cycles.nil?
        if @default_all
          @cycles=[ALL_CYCLES]
        else
          raise OptionParser::ParseError,"At least one cycle must be specified."
        end
      end

      if @tasks.nil? && @metatasks.nil? && ! @all_tasks
        if @default_all
          @all_tasks=true
        else
          raise OptionParser::ParseError,"At least one task or metatask (-t or -m) must be specified, or all tasks (-a)."
        end
      end


      @selection=WorkflowMgr::WorkflowSelection.new(@all_tasks,@tasks,@metatasks,@cycles,@default_all)

    end

  end

end
