##########################################
#
# Module WorkflowMgr
#
##########################################
module WorkflowMgr
  ##########################################
  #
  # Class WorkflowState
  #
  ##########################################
  class WorkflowState
    require 'workflowmgr/stringevaluator'

    ##########################################
    #
    # Readers
    #
    ##########################################
    attr_reader :cycle, :job_list, :workflow_io_server, :cycledefs
    attr_reader :taskname, :task, :tasks

    def taskName
      @taskname
    end

    ##########################################
    #
    # Initialize
    #
    ##########################################
    def initialize(cycle, job_list, workflow_io_server, cycledefs, taskname, task, tasks = nil, doc = nil)
      if taskname.nil?
        raise 'In WorkflowState.new, taskname cannot be nil.'
      end

      @cycle = cycle
      @job_list = job_list
      @workflow_io_server = workflow_io_server
      @cycledefs = cycledefs
      @taskname = taskname
      @task = task
      @tasks = tasks
      @doc = nil
      @se = nil
    end

    ##########################################
    #
    # set_* routines to modify the StringEvaluator
    #
    ##########################################
    def set_cycle(cycle)
      @cycle = cycle
      @se&.set_cycle(cycle)
    end

    def set_task(name, task = nil)
      if taskname.nil?
        raise 'In WorkflowState.set_task, name cannot be nil.'
      end

      @task = task
      @taskname = name
      @se&.set_task(name, task)
    end

    def set_doc(workflowdoc)
      @doc = workflowdoc
      @se&.set_doc(workflowdoc)
    end

    ##########################################
    #
    # ruby_bool
    #
    ##########################################
    def ruby_bool(evalexpr, cycle)
      se(cycle).run_bool(evalexpr)
    rescue SystemCallError => e
      WorkflowMgr.stderr("<rb> tag: SystemCallError: #{e}", 1)
      false
    end

    ##########################################
    #
    # shell_bool
    #
    ##########################################
    def shell_bool(shell, runopt, evalexpr, cycle)
      raise "in shell_bool, shell must be a string" unless shell.is_a?(String)
      raise "in shell_bool, runopt must be a string" unless runopt.is_a?(String)
      raise "in shell_bool, evalexpr must be a string" unless evalexpr.is_a?(String)

      se(cycle).shell_bool(shell, runopt, evalexpr, cycle)
    end

    private

    ##########################################
    #
    # se
    # Create or return the StringEvaluator
    #
    ##########################################
    def se(cycle)
      if @se.nil?
        # Make the StringEvaluator object:
        nse = StringEvaluator.new
        if @taskname.nil?
          raise 'In WorkflowState, @taskname cannot be nil.'
        end

        nse.set_task(@taskname, @task)
        if @cycle.nil?
          raise 'In WorkflowState, @cycle cannot be nil.'
        end

        nse.set_cycle(@cycle)
        nse.setdef('job_list', @job_list)
        nse.setdef('cycledefs', @cycledefs)
        nse.setdef('workflow_io_server', @workflow_io_server)
        @se = nse
      end
      @se
    end
  end
end
