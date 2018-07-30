##########################################
#
# Module WorkflowMgr
#
##########################################
module WorkflowMgr

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

    attr_reader :all_cycles, :all_tasks

    ##########################################  
    #
    # Initialize
    #
    ##########################################
    def initialize(all_cycles,all_tasks,cycles,tasks)
      @all_cycles=!!all_cycles
      @all_tasks=!!all_tasks

      @cycles_array = cycles.to_a
      @cycles_array.sort!
      @cycles_array.uniq!
      @cycles_set = Set.new @cycles_array

      @tasks_array=tasks.to_a
      @tasks_set=Set.new @tasks_array
    end


    ##########################################  
    #
    # !empty? equivalents
    #
    ##########################################
    def cycles?()        return !@cycles_array.empty? ; end
    def tasks?()         return !@tasks_array.empty? ; end


    ##########################################  
    #
    # iterators
    #
    ##########################################
    def each_cycle()        @cycles_array.each {|c| yield c} ; end
    def each_task()         @tasks_array.each {|c|yield c} ; end


    ##########################################  
    #
    # collectors
    #
    ##########################################
    def collect_tasks()        @tasks_array.collect {|c| yield c} ; end
    def collect_cycles()       @cycles_array.collect {|c| yield c} ; end


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

end # module WorkflowMgr
