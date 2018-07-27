##########################################
#
# Module WorkflowMgr
#
##########################################
module WorkflowMgr

  ##########################################
  #
  # Class WorkflowSelection
  #
  ##########################################
  class WorkflowSelection

    require 'workflowmgr/workflowsubset'

    ##########################################  
    #
    # Initialize
    #
    ##########################################
    def initialize(all_tasks=nil,task_selection=[],metatask_selection=[],cycle_selection=[],default_all=false,allow_empty=false)

      all_tasks=default_all if all_tasks.nil?

      # Flags:
      @default_all=!!default_all    # select all tasks and cycles if none are specified
      @all_tasks=!!all_tasks        # from the -a option
      @allow_empty=!!allow_empty    # allow no task or cycle specifications

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

end # module WorkflowMgr
