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
    def initialize(all_tasks = nil, task_options = [], cycle_selection = [], default_all: false, allow_empty: false)
      all_tasks = default_all if all_tasks.nil?

      # Flags:
      @default_all = default_all ? true : false   # select all tasks and cycles if none are specified
      @all_tasks = all_tasks ? true : false       # from the -a option
      @allow_empty = allow_empty ? true : false   # allow no task or cycle specifications

      # Enumerables:
      @task_options = task_options.to_a
      @cycles = cycle_selection

      @tasks = [] if @tasks.nil?
      @cycles = [] if @cycles.nil?
      @metatasks = [] if @metatasks.nil?
    end

    ##########################################
    #
    # add_options
    #
    ##########################################
    def add_options(all_tasks = nil, all_cycles = nil, task_options = [], cycle_selection = [])
      @task_options.concat task_options
      @cycles.concat cycle_selection
      @all_tasks = all_tasks ? true : false unless all_tasks.nil?
      @all_cycles = all_cycles ? true : false unless all_cycles.nil?
    end

    ##########################################
    #
    # make_subset
    #
    ##########################################
    def make_subset(tasks, cycledefs, db_server = nil)
      selected_tasks = select_tasks(tasks)

      cycles = select_cycles(cycledefs)

      WorkflowSubset.new(@all_cycles, @all_tasks, cycles, selected_tasks)
    end

    ##########################################
    #
    # select_cycles
    #
    ##########################################
    def select_cycles(cycledefs)
      # Get the list of boot cycles
      selected_cycles = []

      @cycles.each do |cycopt|
        if cycopt.is_a?(Range)

          # Find every cycle in the range that is a member of a cycledef
          first_times = cycledefs.collect do |cdef|
            cdef.next(cycopt.first, false)
          end
          reftime = first_times.compact.collect { |c| c[0] }.min
          loop do
            break if reftime.nil?
            break if reftime > cycopt.last

            selected_cycles << reftime
            next_times = cycledefs.collect do |cdef|
              cdef.next(reftime + 60, false)
            end
            reftime = next_times.compact.collect { |c| c[0] }.min
          end
        elsif cycopt.is_a? CycleDefSelection
          cycledefs.each do |cdef|
            next unless cycopt.name == cdef.group

            cdef.each(cdef.first, false) do |cyc|
              selected_cycles << cyc
            end
          end
        else
          selected_cycles << cycopt
        end
      end
      selected_cycles.uniq!
      selected_cycles.sort!

      selected_cycles
    end

    ##########################################
    #
    # handle_metatask_selection
    #
    ##########################################
    def handle_metatask_selection(opts, tasks, selection)
      optspec = []
      opts.each do |metaopt|
        negate = false
        if metaopt.start_with? '-'
          negate = true
          metaopt = metaopt[1..]
        end
        optspec << [metaopt, negate]
      end

      tasks.each_value do |task|
        next if task.attributes[:metatasks].nil?

        metatasks = task.attributes[:metatasks].split(',')

        optspec.each do |metaopt, negate|
          if metatasks.include? metaopt
            if negate
              selection.delete(task.attributes[:name])
            else
              selection.add(task.attributes[:name])
            end
          end
        end
      end
    end

    ##########################################
    #
    # handle_task_selection
    #
    ##########################################
    def handle_task_selection(opts, tasks, selection)
      opts.each do |item|
        negate = false
        if item.start_with? '-'
          negate = true
          item = item[1..]
        end

        if item.start_with? ':'
          attribute_name = item[1..]

          case attribute_name
          when 'final'     then attribute = :final
          when 'shared'    then attribute = :shared
          when 'exclusive' then attribute = :exclusive
          when 'metatasks' then attribute = :metatasks
          when 'cores'     then attribute = :cores
          when 'nodes'     then attribute = :nodes
          else
            raise "Unknown attribute '#{attribute_name}' is not one of: " \
                  "final, shared, exclusive, metatasks, cores, nodes"
          end
          tasks.each_value do |task|
            if (negate && !task.attributes[attribute]) || (!negate && task.attributes[attribute])
              if negate
                selection.delete(task.attributes[:name])
              else
                selection.add(task.attributes[:name])
              end
            end
          end
        elsif item.start_with?('/') && item.end_with?('/')
          regex = Regexp.new item[1..-2]
          tasks.each_value do |task|
            if regex =~ task.attributes[:name]
              if negate
                selection.delete(task.attributes[:name])
              else
                selection.add(task.attributes[:name])
              end
            end
          end
        elsif item.start_with? '@'
          cycledef = item[1..]
          tasks.each_value do |task|
            next if task.attributes[:cycledefs].nil?

            cycledefs = task.attributes[:cycledefs].split(',')
            if cycledefs.include? cycledef
              if negate
                selection.delete(task.attributes[:name])
              else
                selection.add(task.attributes[:name])
              end
            end
          end
        elsif negate # explicit task name
          selection.delete(item)
        else
          selection.add(item)
        end
      end
    end

    ##########################################
    #
    # select_tasks
    #
    ##########################################
    def select_tasks(tasks)
      if @all_tasks
        return tasks.values.collect { |task| task.attributes[:name] }.sort
      end

      selection = Set.new
      @task_options.each do |opt|
        if opt.is_a? WorkflowMgr::MetataskSelection
          handle_metatask_selection(opt.arg, tasks, selection)
        else
          handle_task_selection(opt.arg, tasks, selection)
        end
      end
      tasks = selection.to_a
      tasks.sort!
      tasks
    end
  end
end
