##########################################
#
# Module WorkflowMgr
#
##########################################
require 'English'
module WorkflowMgr
  # NOTE: in all of these classes and functions, the variable "d" is a
  # WorkflowMgr::WorkflowState which contains all needed input to the
  # Dependency classes.

  ##########################################
  #
  # Class Dependency
  #
  ##########################################
  class Dependency
    require 'workflowmgr/utilities'

    #####################################################
    #
    # initialize
    #
    #####################################################
    def initialize(root)
      @root = root
    end

    #####################################################
    #
    # Resolved?
    #
    #####################################################
    def resolved?(d)
      @root.resolved?(d)
    rescue WorkflowIOHang
      WorkflowMgr.stderr("#{$ERROR_INFO}", 2)
      WorkflowMgr.log("#{$ERROR_INFO}")
      false
    end

    #####################################################
    #
    # Query
    #
    #####################################################
    def query(d)
      @root.query(d)
    rescue WorkflowIOHang
      WorkflowMgr.stderr("#{$ERROR_INFO}", 2)
      WorkflowMgr.log("#{$ERROR_INFO}")
      false
    end
  end

  ##########################################
  #
  # String Dependency
  #
  ##########################################
  class StringDependency
    # This class represents a string equality or inequality
    # comparison.  The dependency is met if the comparison matches the
    # requirements.  The @compare is the requirement: "==" for
    # equality or "!=" for inequality.  The @left and @right are the
    # CompoundTimeString objects to compare.  The @name is the name to
    # give this comparison.

    attr_reader :left, :right, :name, :compare

    ##########################################
    #
    # initialize
    #
    ##########################################
    def initialize(left, right, name, compare)
      @left = left
      @right = right
      @name = name
      @compare = compare
    end

    ##########################################
    #
    # resolved?
    #
    ##########################################
    def resolved?(d)
      sleft = left.to_s(d.cycle)
      sright = right.to_s(d.cycle)
      if compare == '=='
        sleft == sright
      else
        sleft != sright
      end
    end

    ##########################################
    #
    # query
    #
    ##########################################
    def query(d)
      sleft = left.to_s(d.cycle)
      sright = right.to_s(d.cycle)
      result = if compare == '=='
                 (sleft == sright)
               else
                 (sleft != sright)
               end
      [{ dep: @name, msg: "is #{result}", resolved: result }]
    end
  end

  ##########################################
  #
  # Class ConstDependency
  #
  ##########################################
  class ConstDependency
    # This class represents a simple constant dependency: it is always
    # true or false.

    attr_reader :value, :name

    ##########################################
    #
    # initialize
    #
    ##########################################
    def initialize(value, name)
      @value = !!value
      begin
        @name = name.to_s(Time.new)
      rescue StandardError
        @name = name.to_s
      end
    end

    ##########################################
    #
    # resolved?
    #
    ##########################################
    def resolved?(d)
      @value
    end

    ##########################################
    #
    # query
    #
    ##########################################
    def query(d)
      [{ dep: @name, msg: "is #{value}", resolved: @value }]
    end
  end

  ##########################################
  #
  # Class RubyDependency
  #
  ##########################################
  class RubyDependency
    # This class evaluates a Ruby expression in a boolean context.
    # The true/false value is used to decide whether the dependency is
    # met.  The @name is the name of the dependency, and the @script
    # is the Ruby expression to evaluate.

    # This class can also be used in a <rewind> context, when #rewind!
    # is called.  It works the same way: the expression is evaluated
    # in a logical context, but the logical value is ignored.

    attr_reader :name, :script

    ##########################################
    #
    # initialize
    #
    ##########################################
    def initialize(script, name = nil)
      @script = script
      if name.nil?
        name = script.to_s(Time.new)
        if name.size > 40
          name = "#{name[0..37]}..."
        end
      end
      @name = name
    end

    ##########################################
    #
    # query
    #
    ##########################################
    def query(d)
      if resolved?(d)
        [{ dep: @name, msg: "returned true", resolved: true }]
      else
        [{ dep: @name, msg: "returned false", resolved: false }]
      end
    end

    ##########################################
    #
    # resolved?
    #
    ##########################################
    def resolved?(d)
      d.ruby_bool(@script.to_s(d.cycle), d.cycle)
    end

    ##########################################
    #
    # rewind!
    #
    ##########################################
    def rewind!(d)
      d.ruby_bool(@script.to_s(d.cycle), d.cycle)
    end
  end

  ##########################################
  #
  # Class ShellDependency
  #
  ##########################################
  class ShellDependency
    # This class executes a shell expression, by sending it to sh -c
    # (or some other interpreter, if requested).  A successful
    # execution with a 0 return value is treated as true, and anything
    # else is false.  This class can also be used in a <rewind>
    # context (#rewind!) and fuctions the same way, except that the
    # return value of the program is ignored.

    attr_reader :name, :script

    ##########################################
    #
    # initialize
    #
    ##########################################
    def initialize(shell, runopt, shellexpr, name = nil)
      raise 'In ShellDependency, runopt must not be nil' if runopt.nil?
      raise 'In ShellDependency, shell must not be nil' if shell.nil?
      raise 'In ShellDependency, shellexpr must not be nil' if shellexpr.nil?
      raise 'In ShellDependency, shellexpr must be a CompoundTimeString' unless shellexpr.is_a?(CompoundTimeString)

      @shellexpr = shellexpr
      @runopt = runopt
      @shell = shell
      if name.nil?
        name = shellexpr.to_s(Time.new)
        if name.size > 40
          name = "#{name[0..37]}..."
        end
      end
      @name = name
    end

    ##########################################
    #
    # query
    #
    ##########################################
    def query(d)
      if resolved?(d)
        [{ dep: @name, msg: "returned true", resolved: true }]
      else
        [{ dep: @name, msg: "returned false", resolved: false }]
      end
    end

    ##########################################
    #
    # resolved?
    #
    ##########################################
    def resolved?(d)
      ex = @shellexpr.to_s(d.cycle)
      d.shell_bool(@shell, @runopt, ex, d.cycle)
    end

    ##########################################
    #
    # rewind!
    #
    ##########################################
    def rewind!(d)
      ex = @shellexpr.to_s(d.cycle)
      d.shell_bool(@shell, @runopt, ex, d.cycle)
    end
  end

  ##########################################
  #
  # Class Dependency_NOT_Operator
  #
  ##########################################
  class Dependency_NOT_Operator
    #####################################################
    #
    # initialize
    #
    #####################################################
    def initialize(operand)
      raise "Not operator contains more than one operand!" if operand.size > 1

      @operand = operand.first
    end

    #####################################################
    #
    # resolved?
    #
    #####################################################
    def resolved?(d)
      !@operand.resolved?(d)
    end

    #####################################################
    #
    # query
    #
    #####################################################
    def query(d)
      query = @operand.query(d)
      if query.first[:resolved]
        [{ dep: "NOT", msg: "is not satisfied", resolved: false }, query]
      else
        [{ dep: "NOT", msg: "is satisfied", resolved: true }, query]
      end
    end
  end

  ##########################################
  #
  # Class Dependency_AND_Operator
  #
  ##########################################
  class Dependency_AND_Operator
    #####################################################
    #
    # initialize
    #
    #####################################################
    def initialize(operands)
      @operands = operands
    end

    #####################################################
    #
    # resolved?
    #
    #####################################################
    def resolved?(d)
      @operands.each do |operand|
        return false unless operand.resolved?(d)
      end
      true
    end

    #####################################################
    #
    # query
    #
    #####################################################
    def query(d)
      queries = []
      @operands.each do |operand|
        query = operand.query(d)
        queries += query
        unless query.first[:resolved]
          return [{ dep: "AND", msg: "is not satisfied", resolved: false },
                  queries]
        end
      end
      [{ dep: "AND", msg: "is satisfied", resolved: true }, queries]
    end
  end

  ##########################################
  #
  # Class Dependency_OR_Operator
  #
  ##########################################
  class Dependency_OR_Operator
    #####################################################
    #
    # initialize
    #
    #####################################################
    def initialize(operands)
      @operands = operands
    end

    #####################################################
    #
    # resolved?
    #
    #####################################################
    def resolved?(d)
      @operands.each do |operand|
        return true if operand.resolved?(d)
      end
      false
    end

    #####################################################
    #
    # query
    #
    #####################################################
    def query(d)
      queries = []
      @operands.each do |operand|
        query = operand.query(d)
        queries += query
        return [{ dep: "OR", msg: "is satisfied", resolved: true }, queries] if query.first[:resolved]
      end
      [{ dep: "OR", msg: "is not satisfied", resolved: false }, queries]
    end
  end

  ##########################################
  #
  # Class Dependency_NAND_Operator
  #
  ##########################################
  class Dependency_NAND_Operator
    #####################################################
    #
    # initialize
    #
    #####################################################
    def initialize(operands)
      @operands = operands
    end

    #####################################################
    #
    # resolved?
    #
    #####################################################
    def resolved?(d)
      @operands.each do |operand|
        return true unless operand.resolved?(d)
      end
      false
    end

    #####################################################
    #
    # query
    #
    #####################################################
    def query(d)
      queries = []
      @operands.each do |operand|
        query = operand.query(d)
        queries += query
        return [{ dep: "<nand>", msg: "is satisfied", resolved: true }, queries] unless query.first[:resolved]
      end
      [{ dep: "<nand>", msg: "is not satisfied", resolved: false }, queries]
    end
  end

  ##########################################
  #
  # Class Dependency_NOR_Operator
  #
  ##########################################
  class Dependency_NOR_Operator
    #####################################################
    #
    # initialize
    #
    #####################################################
    def initialize(operands)
      @operands = operands
    end

    #####################################################
    #
    # resolved?
    #
    #####################################################
    def resolved?(d)
      @operands.each do |operand|
        return false if operand.resolved?(d)
      end
      true
    end

    #####################################################
    #
    # query
    #
    #####################################################
    def query(d)
      queries = []
      @operands.each do |operand|
        query = operand.query(d)
        queries += query
        return [{ dep: "NOR", msg: "is not satisfied", resolved: false }, queries] if query.first[:resolved]
      end
      [{ dep: "NOR", msg: "is satisfied", resolved: true }, queries]
    end
  end

  ##########################################
  #
  # Class Dependency_XOR_Operator
  #
  ##########################################
  class Dependency_XOR_Operator
    #####################################################
    #
    # initialize
    #
    #####################################################
    def initialize(operands)
      @operands = operands
    end

    #####################################################
    #
    # resolved?
    #
    #####################################################
    def resolved?(d)
      ntrue = 0
      @operands.each do |operand|
        ntrue += 1 if operand.resolved?(d)
        return false if ntrue > 1
      end
      ntrue == 1
    end

    #####################################################
    #
    # query
    #
    #####################################################
    def query(d)
      queries = []
      ntrue = 0
      @operands.each do |operand|
        query = operand.query(d)
        queries += query
        ntrue += 1 if query.first.resolved?(d)
        return [{ dep: "XOR", msg: "is not satisfied", resolved: false }, queries] if ntrue > 1
      end
      if ntrue == 1
        [{ dep: "XOR", msg: "is satisfied", resolved: true }, queries]
      else
        [{ dep: "XOR", msg: "is not satisfied", resolved: false }, queries]
      end
    end
  end

  ##########################################
  #
  # Class Dependency_SOME_Operator
  #
  ##########################################
  class Dependency_SOME_Operator
    #####################################################
    #
    # initialize
    #
    #####################################################
    def initialize(operands, threshold = 1.0)
      @operands = operands
      @threshold = threshold
    end

    #####################################################
    #
    # resolved?
    #
    #####################################################
    def resolved?(d)
      ntrue = 0.0
      @operands.each do |operand|
        ntrue += 1.0 if operand.resolved?(d)
        return true if ntrue / @operands.size >= @threshold
      end
      false
    end

    #####################################################
    #
    # query
    #
    #####################################################
    def query(d)
      queries = []
      ntrue = 0.0
      @operands.each do |operand|
        query = operand.query(d)
        queries += query
        ntrue += 1.0 if query.first[:resolved]
        if ntrue / @operands.size >= @threshold
          return [{ dep: "SOME", msg: "is satisfied", resolved: true },
                  queries]
        end
      end
      [{ dep: "SOME", msg: "is not satisfied", resolved: false }, queries]
    end
  end

  ##########################################
  #
  # Class TaskDependency
  #
  ##########################################
  class TaskDependency
    #####################################################
    #
    # initialize
    #
    #####################################################
    def initialize(task, state, cycle_offset)
      @task = task
      @state = state.upcase
      @cycle_offset = cycle_offset
    end

    #####################################################
    #
    # Resolved?
    #
    #####################################################
    def resolved?(d)
      return false if d.jobList.nil?
      return false if d.jobList[@task].nil?
      return false if d.jobList[@task][d.cycle.getgm + @cycle_offset].nil?

      d.jobList[@task][d.cycle.getgm + @cycle_offset].state == @state
    end

    #####################################################
    #
    # Query
    #
    #####################################################
    def query(d)
      if d.jobList.nil?
        return [{ dep: "#{@task} of cycle #{(d.cycle.getgm + @cycle_offset).strftime('%Y%m%d%H%M')}",
                  msg: "is not #{@state}", resolved: false }]
      end
      if d.jobList[@task].nil?
        return [{ dep: "#{@task} of cycle #{(d.cycle.getgm + @cycle_offset).strftime('%Y%m%d%H%M')}",
                  msg: "is not #{@state}", resolved: false }]
      end
      if d.jobList[@task][d.cycle.getgm + @cycle_offset].nil?
        return [{ dep: "#{@task} of cycle #{(d.cycle.getgm + @cycle_offset).strftime('%Y%m%d%H%M')}",
                  msg: "is not #{@state}", resolved: false }]
      end
      if d.jobList[@task][d.cycle.getgm + @cycle_offset].state == @state
        [{ dep: "#{@task} of cycle #{(d.cycle.getgm + @cycle_offset).strftime('%Y%m%d%H%M')}",
           msg: "is #{@state}", resolved: true }]
      else
        [{ dep: "#{@task} of cycle #{(d.cycle.getgm + @cycle_offset).strftime('%Y%m%d%H%M')}",
           msg: "is not #{@state}", resolved: false }]
      end
    end
  end

  ##########################################
  #
  # Class CycleExistDependency
  #
  ##########################################
  class CycleExistDependency
    #####################################################
    #
    # initialize
    #
    #####################################################
    def initialize(cycle_offset)
      @cycle_offset = cycle_offset
    end

    #####################################################
    #
    # Resolved?
    #
    #####################################################
    def resolved?(d)
      relcycle = d.cycle.getgm + @cycle_offset
      d.cycledefs.each do |cycledef|
        if cycledef.member?(relcycle)
          return true
        end
      end
      false
    end

    #####################################################
    #
    # Query
    #
    #####################################################
    def query(d)
      relcycle = d.cycle.getgm + @cycle_offset
      d.cycledefs.each do |cycledef|
        if cycledef.member?(relcycle)
          return [{ dep: "cycle #{relcycle.strftime('%Y%m%d%H%M')}", msg: "exists", resolved: true }]
        end
      end
      [{ dep: "cycle #{relcycle.strftime('%Y%m%d%H%M')}", msg: "does not exist", resolved: false }]
    end
  end

  ##########################################
  #
  # Class TaskValidDependency
  #
  ##########################################
  class TaskValidDependency
    #####################################################
    #
    # initialize
    #
    #####################################################
    def initialize(task)
      @task = task
    end

    #####################################################
    #
    # Resolved?
    #
    #####################################################
    def resolved?(d)
      # Get the jobs for this cycle
      return false if d.tasks[@task].nil?

      checkjob = d.tasks[@task]

      # Get the mandatory task attribute
      cycle_is_valid = true
      unless checkjob.attributes[:cycledefs].nil?
        taskcycledefs = d.cycledefs.find_all do |cycledef|
          checkjob.attributes[:cycledefs].split(/[\s,]+/).member?(cycledef.group)
        end
        # Cycle is invalid for this task if the cycle is not a member of the tasks cycle list
        unless taskcycledefs.any? { |cycledef| cycledef.member?(d.cycle) }
          cycle_is_valid = false
        end
      end

      cycle_is_valid
    end

    #####################################################
    #
    # Query
    #
    #####################################################
    def query(d)
      # Set the job to check

      # Get the jobs for this cycle
      return [{ dep: "#{@task}", msg: "is not valid", resolved: false }] if d.tasks[@task].nil?

      checkjob = d.tasks[@task]

      cycle_is_valid = true
      unless checkjob.attributes[:cycledefs].nil?
        taskcycledefs = d.cycledefs.find_all do |cycledef|
          checkjob.attributes[:cycledefs].split(/[\s,]+/).member?(cycledef.group)
        end
        # Cycle is invalid for this task if the cycle is not a member of the tasks cycle list
        unless taskcycledefs.any? { |cycledef| cycledef.member?(d.cycle) }
          cycle_is_valid = false
        end
      end

      if cycle_is_valid
        [{ dep: "#{@task}", msg: "is valid", resolved: true }]
      else
        [{ dep: "#{@task}", msg: "is not valid", resolved: false }]
      end
    end
  end

  ##########################################
  #
  # Class TimeDependency
  #
  ##########################################
  class TimeDependency
    #####################################################
    #
    # initialize
    #
    #####################################################
    def initialize(timestr)
      @timestr = timestr
    end

    #####################################################
    #
    # Resolved?
    #
    #####################################################
    def resolved?(d)
      timestr = @timestr.to_s(d.cycle)
      t = Time.gm(timestr[0..3],
                  timestr[4..5],
                  timestr[6..7],
                  timestr[8..9],
                  timestr[10..11],
                  timestr[12..13])

      Time.now.getgm > t
    end

    #####################################################
    #
    # Query
    #
    #####################################################
    def query(d)
      timestr = @timestr.to_s(d.cycle)
      t = Time.gm(timestr[0..3],
                  timestr[4..5],
                  timestr[6..7],
                  timestr[8..9],
                  timestr[10..11],
                  timestr[12..13])

      if Time.now.getgm > t
        [{ dep: "Walltime", msg: "is > #{t}", resolved: true }]
      else
        [{ dep: "Walltime", msg: "is <= #{t}", resolved: false }]
      end
    end
  end

  ##########################################
  #
  # Class DataDependency
  #
  ##########################################
  class DataDependency
    #####################################################
    #
    # initialize
    #
    #####################################################
    def initialize(datapath, age, minsize)
      @datapath = datapath
      @age = age
      @minsize = minsize
    end

    #####################################################
    #
    # Resolved?
    #
    #####################################################
    def resolved?(d)
      filename = @datapath.to_s(d.cycle)
      if d.workflowIOServer.exist?(filename)
        if d.workflowIOServer.size(filename) >= @minsize
          Time.now > (d.workflowIOServer.mtime(filename) + @age)
        else
          false
        end
      else
        false
      end
    end

    #####################################################
    #
    # Query
    #
    #####################################################
    def query(d)
      filename = @datapath.to_s(d.cycle)
      if d.workflowIOServer.exist?(filename)
        if Time.now > (d.workflowIOServer.mtime(filename) + @age)
          if d.workflowIOServer.size(filename) >= @minsize
            [{ dep: filename, msg: "is available", resolved: true }]
          else
            [{ dep: filename, msg: "is not large enough", resolved: false }]
          end
        else
          [{ dep: filename, msg: "is not old enough", resolved: false }]
        end
      else
        [{ dep: filename, msg: "does not exist", resolved: false }]
      end
    end
  end
end
