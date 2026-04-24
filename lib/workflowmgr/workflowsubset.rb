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
    require 'workflowmgr/cycle'
    require 'workflowmgr/task'
    require 'workflowmgr/job'

    attr_reader :all_cycles, :all_tasks

    ##########################################
    #
    # Initialize
    #
    ##########################################
    def initialize(all_cycles, all_tasks, cycles, tasks)
      @all_cycles = all_cycles ? true : false
      @all_tasks = all_tasks ? true : false

      @cycles_array = cycles.to_a
      @cycles_array.sort!
      @cycles_array.uniq!
      @cycles_set = Set.new @cycles_array

      @tasks_array = tasks.to_a
      @tasks_set = Set.new @tasks_array
    end

    ##########################################
    #
    # !empty? equivalents
    #
    ##########################################
    def cycles? = !@cycles_array.empty?
    def tasks? = !@tasks_array.empty?


    ##########################################
    #
    # iterators
    #
    ##########################################
    def each_cycle(&block) = @cycles_array.each(&block)
    def each_task(&block) = @tasks_array.each(&block)


    ##########################################
    #
    # collectors
    #
    ##########################################
    def collect_tasks(&block) = @tasks_array.collect(&block)
    def collect_cycles(&block) = @cycles_array.collect(&block)

    ##########################################
    #
    # selected?
    #
    ##########################################
    def selected?(arg)
      return true if @all_cycles && @all_tasks

      case arg
      when WorkflowMgr::Cycle
        return true if @all_cycles

        @cycles_set.include? arg.cycle
      when String
        return true if @all_tasks

        @tasks_set.include? arg
      when WorkflowMgr::Task
        return true if @all_tasks

        @tasks_set.include? arg.attributes[:name]
      when Time
        return true if @all_cycles

        @cycles_set.include? arg
      when WorkflowMgr::Job
        (@all_cycles || @cycles_set.include?(arg.cycle)) &&
          (@all_tasks || @tasks_set.include?(arg.task.attributes[:name]))
      when Range
        raise "Unexpected type #{arg.class.name} in \"selected?\".  " \
              "Querying Ranges of cycles is not yet implemented."
      when Enumerable
        arg.all? { |elem| selected? elem }
      else
        raise "Unexpected type #{arg.class.name} in \"selected?\".  " \
              "Only Cycle, Task, String (task name), Time, Job, and Enumerables thereof " \
              "(except Ranges) are allowed."
      end
    end
  end
end
