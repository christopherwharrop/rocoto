##########################################
#
# Module WorkflowMgr
#
##########################################
module WorkflowMgr

  # Two classes here implement two steps of selecting a subset of the
  # workflow, and a third class connects it to command-line arguments.
  #
  #   WorkflowSelection - this is the input.  It stores the details of
  #   your desired subset and knows how to produce the resulting list
  #   of tasks and cycles.
  #
  #   WorkflowSubset - this is the output.  It knows which cycles and
  #   tasks are selected by your WorkflowSelection by analyzing the
  #   workflow document and batch system information.  You can query
  #   it for whether a task, cycle, or job is part of the subset.
  # 
  #   WorkflowSubsetOptions - parses the -t, -m, -c, and -a options of
  #   commands like rocotoboot and rocotorun.  Generates a
  #   WorkflowSelection.

  require 'workflowmgr/workflowoption'

  ##########################################
  #
  # Class WorkflowSelection
  #
  ##########################################
  class WorkflowSelection

    ##########################################  
    #
    # Initialize
    #
    ##########################################
    def initialize(all_tasks=nil,task_selection=[],metatask_selection=[],cycle_selection=[],default_all=false)

      all_tasks=default_all if all_tasks.nil?

      # Flags:
      @default_all=!!default_all    # select all tasks and cycles if none are specified
      @all_tasks=!!all_tasks        # from the -a option

      # Enumerables:
      @tasks=task_selection         # from parsing the -t options
      @cycles=cycle_selection       # from parsing the -c options
      @metatasks=metatask_selection # from parsing the -m options

      @tasks=[] if @tasks.nil?
      @cycles=[] if @cycles.nil?
      @metatasks=[] if @metatasks.nil?
    end


    ##########################################  
    #
    # add_options
    #
    ##########################################
    def add_options(all_tasks=nil,all_cycles=nil,task_selection=[],metatask_selection=[],cycle_selection=[])
      @tasks.concat task_selection
      @cycles.concat cycle_selection
      @metatasks.concat metatask_selection
      @all_tasks=!!all_tasks unless all_tasks.nil?
      @all_cycles=!!all_cycles unless all_cycles.nil?
    end

    ##########################################  
    #
    # make_subset
    #
    ##########################################
    def make_subset(tasks,cycledefs)
      return WorkflowSubset.new(select_cycles(cycledefs),select_tasks(tasks),@all_tasks,@all_cycles)
    end

  private

    ##########################################  
    #
    # select_cycles
    #
    ##########################################
    def select_cycles(cycledefs)
      # Get the list of boot cycles
      selected_cycles=[]
      @cycles.each do |cycopt|
        if cycopt.is_a?(Range)

          # Find every cycle in the range that is a member of a cycledef
          reftime=cycledefs.collect { |cdef| cdef.next(cycopt.first,by_activation_time=false) }.compact.collect {|c| c[0] }.min
          while true do
            break if reftime.nil?
            break if reftime > cycopt.last
            selected_cycles << reftime
            reftime=cycledefs.collect { |cdef| cdef.next(reftime+60,by_activation_time=false) }.compact.collect {|c| c[0] }.min
          end
          
        else
          selected_cycles << cycopt
        end
      end
      selected_cycles.uniq!
      selected_cycles.sort!

      return selected_cycles
    end


    ##########################################  
    #
    # select_tasks
    #
    ##########################################
    def select_tasks(tasks)
      return tasks.keys if @all_tasks

      pass1=@tasks || []
      tasks.values.find_all { |t| !t.attributes[:metatasks].nil? }.each { |t|
        pass1 << t.attributes[:name] unless (t.attributes[:metatasks].split(",") & @metatasks).empty?
      } unless @metatasks.nil?

      pass2=[]
      pass1.each do |item|
        if item.start_with? ':'
          negate=false
          if item[1..1]=='!'
            attribute_name=item[2..-1]
            negate=true
          else
            attribute_name=item[1..-1]
          end

          case attribute_name
            when 'final'     then attribute=:final
            when 'shared'    then attribute=:shared
            when 'exclusive' then attribute=:exclusive
            when 'metatasks' then attribute=:metatasks
            when 'cores'     then attribute=:cores
            when 'nodes'     then attribute=:nodes
          else
            raise "Unknown attribute '#{attribute_name}' is not one of: final, shared, exclusive, metatasks, cores, nodes"
          end
          tasks.values.each do |task|
            if ( negate && ! task.attributes[attribute] ) || (!negate && task.attributes[attribute])
              pass2 << task.attributes[:name]
            end
          end
        elsif item.start_with? '/' and item.end_with? '/'
          regex=Regexp.new item[1..-2]
          tasks.values.each do |task|
            if regex=~task.attributes[:name]
              pass2 << task.attributes[:name]
            end
          end
        elsif item.start_with? '@'
          cycledef=item[1..-1]
          tasks.values.each do |task|
            next if task.attributes[:cycledefs].nil?
            cycledefs=task.attributes[:cycledefs].split(',')
            if cycledefs.include? cycledef
              pass2 << task.attributes[:name] 
            end
          end
        else
          pass2 << item
        end
      end

      selected_tasks=pass2

      selected_tasks.uniq!
      selected_tasks.sort! { |t1,t2| tasks[t1].seq <=> tasks[t2].seq }

      return selected_tasks
    end
  end # class WorkflowSelection


  # --------------------------------------------------------------------


  ##########################################
  #
  # Class WorkflowSubset
  #
  ##########################################
  class WorkflowSubset

    require 'set'
    require 'workflowmgr/cycle'
    require 'workflowmgr/task'
    require 'workflowmgr/job'

    ##########################################  
    #
    # Initialize
    #
    ##########################################
    def initialize(cycles,tasks,all_cycles,all_tasks)
      @cycles_array=Array.new cycles
      @tasks_array=Array.new tasks

      @cycles_set=Set.new cycles
      @tasks_set=Set.new tasks

      @all_cycles=!!all_cycles
      @all_tasks=!!all_tasks
    end


    ##########################################  
    #
    # each_cycle
    #
    ##########################################
    def each_cycle
      @cycles_array.each do |cycle|
        yield cycle
      end
    end


    ##########################################  
    #
    # each_task
    #
    ##########################################
    def each_task
      @task_array.each do |task|
        yield task
      end
    end


    ##########################################  
    #
    # collect_tasks = @tasks_array.collect
    #
    ##########################################
    def collect_tasks
      return @tasks_array.collect { |task| yield(task) }
    end


    ##########################################  
    #
    # collect_cycles = @cycles_array.collect
    #
    ##########################################
    def collect_cycles
      return @cycles_array.collect { |cycle| yield(cycle) }
    end


    ##########################################
    #
    # is_selected
    #
    ##########################################
    def is_selected?(arg)

      return true if @all_cycles and @all_tasks

      case arg
      when WorkflowMgr::Cycle
        return true if @all_cycles
        return @cycles_set.include? arg.cycle
      when String
        return true if @all_tasks
        return @tasks_set.include? arg
      when WorkflowMgr::Task
        return true if @all_tasks
        return @tasks_set.include? arg.attributes[:name]
      when Time
        return true if @all_cycles
        return @cycles_set.include? arg
      when WorkflowMgr::Job
        return( ( @all_cycles || @cycles_set.include?(job.cycle) ) && ( @all_tasks || @tasks_set.include?(job.task.attributes[:name]) ) )
      when Range
        raise "Unexpected type #{arg.class.name} in \"is_selected?\".  Querying Ranges of cycles is not yet implemented."
      when Enumerable
        return arg.all? { |elem| is_selected? elem }
      else
        raise "Unexpected type #{arg.class.name} in \"is_selected?\".  Only Cycle, Task, String (task name), Time, Job, and Enumerables thereof (except Ranges) are allowed."
      end

    end

  end # class WorkflowSubset


  # --------------------------------------------------------------------


  ##########################################  
  #
  # Class WorkflowSubsetOptions
  #
  ##########################################
  class WorkflowSubsetOptions < WorkflowOption

    ALL_CYCLES=(Time.gm(1900,1,1,0,0)..Time.gm(9999,12,31,23,59))

    require 'workflowmgr/workflowoption'

    attr_reader :database, :workflowdoc, :cycles, :tasks, :metatasks, :verbose, :all_tasks, :selection

    ##########################################  
    #
    # Initialize
    #
    ##########################################
    def initialize(args,name,action,default_all=false)

      @cycles=nil
      @tasks=nil
      @metatasks=nil
      @default_all=default_all  # true => command defaults to all tasks and cycles
      @name=name # ie.: rocotoboot
      @action=action # ie.: boot
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


      @selection=WorkflowSelection.new(@all_tasks,@tasks,@metatasks,@cycles,@default_all)

    end

  end


end # module WorkflowSubsetting
