##########################################
#
# Module WorkflowMgr
#
##########################################
module WorkflowMgr

  require 'workflowmgr/workflowsubset'
  require 'workflowmgr/selectionutil'

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
    def initialize(all_tasks=nil,task_options=[],cycle_selection=[],default_all=false,allow_empty=false)

      all_tasks=default_all if all_tasks.nil?

      # Flags:
      @default_all=!!default_all    # select all tasks and cycles if none are specified
      @all_tasks=!!all_tasks        # from the -a option
      @allow_empty=!!allow_empty    # allow no task or cycle specifications

      # Enumerables:
      @task_options=task_options.to_a
      @cycles=cycle_selection

      @tasks=[] if @tasks.nil?
      @cycles=[] if @cycles.nil?
      @metatasks=[] if @metatasks.nil?
    end


    ##########################################
    #
    # add_options
    #
    ##########################################
    def add_options(all_tasks=nil,all_cycles=nil,task_options=[],cycle_selection=[])
      @task_options.concat task_options
      @cycles.concat cycle_selection
      @all_tasks=!!all_tasks unless all_tasks.nil?
      @all_cycles=!!all_cycles unless all_cycles.nil?
    end

    ##########################################
    #
    # make_subset
    #
    ##########################################
    def make_subset(tasks,cycledefs,dbServer=nil)
      selected_tasks=select_tasks(tasks)

      cycles=select_cycles(cycledefs)

      return WorkflowSubset.new(@all_cycles,@all_tasks,cycles,selected_tasks)
    end

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
        elsif cycopt.is_a? CycleDefSelection
          cycledefs.each do |cdef|
            if cycopt.name == cdef.group
              cdef.each(cdef.first,by_activation_time=false) do |cyc|
                selected_cycles << cyc
              end
            end
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
    # handle_metatask_selection
    #
    ##########################################
    def handle_metatask_selection(opts,tasks,selection)
      optspec=[]
      opts.each do |metaopt|
        negate=false
        if metaopt.start_with? '-'
          negate=true
          metaopt=metaopt[1..-1]
        end
        optspec << [metaopt,negate]
      end # each option

      tasks.values.each do |task|
        next if task.attributes[:metatasks].nil?
        metatasks=task.attributes[:metatasks].split(',')

        optspec.each do |metaopt,negate|
          if metatasks.include? metaopt
            if negate
              selection.delete(task.attributes[:name])
            else
              selection.add(task.attributes[:name])
            end
          end
        end # each option
      end # each task
    end

    ##########################################
    #
    # handle_task_selection
    #
    ##########################################
    def handle_task_selection(opts,tasks,selection)
      optspec=[]
      opts.each do |item|
        negate=false
        if item.start_with? '-'
          negate=true
          item=item[1..-1]
        end

        if item.start_with? ':'
          attribute_name=item[1..-1]

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
              if negate
                selection.delete(task.attributes[:name])
              else
                selection.add(task.attributes[:name])
              end
            end
          end
        elsif item.start_with? '/' and item.end_with? '/'
          regex=Regexp.new item[1..-2]
          tasks.values.each do |task|
            if regex=~task.attributes[:name]
              if negate
                selection.delete(task.attributes[:name])
              else
                selection.add(task.attributes[:name])
              end
            end
          end
        elsif item.start_with? '@'
          cycledef=item[1..-1]
          tasks.values.each do |task|
            next if task.attributes[:cycledefs].nil?
            cycledefs=task.attributes[:cycledefs].split(',')
            if cycledefs.include? cycledef
              if negate
                selection.delete(task.attributes[:name])
              else
                selection.add(task.attributes[:name])
              end
            end
          end
        else # explicit task name
          if negate
            selection.delete(item)
          else
            selection.add(item)
          end
        end
      end # each option
    end


    ##########################################
    #
    # select_tasks
    #
    ##########################################
    def select_tasks(tasks)
      if @all_tasks
        return tasks.values.collect{|task| task.attributes[:name]}.sort
      end
      selection=Set.new
      @task_options.each do |opt|
        if opt.is_a? WorkflowMgr::MetataskSelection
          handle_metatask_selection(opt.arg,tasks,selection)
        else
          handle_task_selection(opt.arg,tasks,selection)
        end
      end
      tasks=selection.to_a
      tasks.sort!
      return tasks
    end
  end # class WorkflowSelection

end # module WorkflowMgr
