##########################################
#
# Module WorkflowMgr
#
##########################################
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

      @root=root

    end

    #####################################################
    #
    # Resolved?
    #
    #####################################################
    def resolved?(d)

      begin
        return(@root.resolved?(d))
      rescue WorkflowIOHang
        WorkflowMgr.stderr("#{$!}",2)
        WorkflowMgr.log("#{$!}")
        return false
      end

    end

    #####################################################
    #
    # Query
    #
    #####################################################
    def query(d)

      begin
        return(@root.query(d))
      rescue WorkflowIOHang
        WorkflowMgr.stderr("#{$!}",2)
        WorkflowMgr.log("#{$!}")
        return false
      end

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
    def initialize(left,right,name,compare)
      @left=left
      @right=right
      @name=name
      @compare=compare
    end

    ##########################################
    # 
    # resolved?
    #
    ##########################################
    def resolved?(d)
      sleft=left.to_s(d.cycle)
      sright=right.to_s(d.cycle)
      if compare=='=='
          return sleft==sright
      else
          return sleft!=sright
      end
    end

    ##########################################
    #
    # query
    #
    ##########################################
    def query(d)
      sleft=left.to_s(d.cycle)
      sright=right.to_s(d.cycle)
      if compare=='=='
          result=(sleft==sright)
      else
          result=(sleft!=sright)
      end
      return [{:dep=>@name, :msg=>"is #{result}", :resolved=>result}]
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
    def initialize(value,name)
      @value=!!value
      @name=name.to_s(Time.new)
    end

    ##########################################
    #
    # resolved?
    #
    ##########################################
    def resolved?(d)
      return @value
    end
    
    ##########################################
    #
    # query
    #
    ##########################################
    def query(d)
      return [{:dep=>@name, :msg=>"is #{value}", :resolved=>@value}]
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
    def initialize(script,name=nil)
      @script=script
      if name.nil?
        name=script.to_s(Time.new)
        if name.size>40
          name=name[0..37]+'...'
        end
      end
      @name=name
    end

    ##########################################
    #
    # query
    #
    ##########################################
    def query(d)
      if resolved?(d)
        return [{:dep=>@name, :msg=>"returned true", :resolved=>true }]
      else
        return [{:dep=>@name, :msg=>"returned false", :resolved=>false }]
      end
    end

    ##########################################
    #
    # resolved?
    #
    ##########################################
    def resolved?(d)
      return d.ruby_bool(@script.to_s(d.cycle),d.cycle)
    end

    ##########################################
    #
    # rewind!
    #
    ##########################################
    def rewind!(d)
      d.ruby_bool(@script.to_s(d.cycle),d.cycle)
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
    def initialize(shell,runopt,shellexpr,name=nil)
      raise 'In ShellDependency, runopt must not be nil' if runopt.nil?
      raise 'In ShellDependency, shell must not be nil' if shell.nil?
      raise 'In ShellDependency, shellexpr must not be nil' if shellexpr.nil?
      raise 'In ShellDependency, shellexpr must be a CompoundTimeString' unless shellexpr.is_a?(CompoundTimeString)
      @shellexpr=shellexpr
      @runopt=runopt
      @shell=shell
      if name.nil?
        name=shellexpr.to_s(Time.new)
        if name.size>40
          name=name[0..37]+'...'
        end
      end
      @name=name
    end

    ##########################################
    #
    # query
    #
    ##########################################
    def query(d)
      if resolved?(d)
        return [{:dep=>@name, :msg=>"returned true", :resolved=>true }]
      else
        return [{:dep=>@name, :msg=>"returned false", :resolved=>false }]
      end
    end

    ##########################################
    #
    # resolved?
    #
    ##########################################
    def resolved?(d)
      ex=@shellexpr.to_s(d.cycle)
      puts "#{@shell} #{@runopt} #{ex} (#{d.cycle})"
      return d.shell_bool(@shell,@runopt,ex,d.cycle)
    end

    ##########################################
    #
    # rewind!
    #
    ##########################################
    def rewind!(d)
      ex=@shellexpr.to_s(d.cycle)
      puts "#{@shell} #{@runopt} #{ex} (#{d.cycle})"
      d.shell_bool(@shell,@runopt,ex,d.cycle)
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

      @operand=operand.first

    end

    #####################################################
    #
    # resolved?
    #
    #####################################################
    def resolved?(d)

      return !@operand.resolved?(d)

    end

    #####################################################
    #
    # query
    #
    #####################################################
    def query(d)
      query=@operand.query(d)
      if query.first[:resolved]
        return [{:dep=>"NOT", :msg=>"is not satisfied", :resolved=>false }, query]
      else
        return [{:dep=>"NOT", :msg=>"is satisfied", :resolved=>true }, query]
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

      @operands=operands

    end

    #####################################################
    #
    # resolved?
    #
    #####################################################
    def resolved?(d)

      @operands.each { |operand|
        return false unless operand.resolved?(d)
      }
      return true

    end


    #####################################################
    #
    # query
    #
    #####################################################
    def query(d)

      queries=[]
      @operands.each { |operand|
        query = operand.query(d)
        queries += query
        return [{:dep=>"AND", :msg=>"is not satisfied", :resolved=>false }, queries] unless query.first[:resolved]
      }
      return [{:dep=>"AND", :msg=>"is satisfied", :resolved=>true }, queries]

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

      @operands=operands

    end

    #####################################################
    #
    # resolved?
    #
    #####################################################
    def resolved?(d)

      @operands.each { |operand|
        return true if operand.resolved?(d)
      }
      return false

    end


    #####################################################
    #
    # query
    #
    #####################################################
    def query(d)

      queries=[]
      @operands.each { |operand|
        query = operand.query(d)
        queries += query
        return [{:dep=>"OR", :msg=>"is satisfied", :resolved=>true }, queries] if query.first[:resolved]
      }
      return [{:dep=>"OR", :msg=>"is not satisfied", :resolved=>false }, queries]

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

      @operands=operands

    end

    #####################################################
    #
    # resolved?
    #
    #####################################################
    def resolved?(d)

      @operands.each { |operand|
        return true if !operand.resolved?(d)
      }
      return false

    end

    #####################################################
    #
    # query
    #
    #####################################################
    def query(d)

      queries=[]
      @operands.each { |operand|
        query = operand.query(d)
        queries += query
        return [{:dep=>"<nand>", :msg=>"is satisfied", :resolved=>true }, queries] if !query.first[:resolved]
      }
      return [{:dep=>"<nand>", :msg=>"is not satisfied", :resolved=>false }, queries]

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

      @operands=operands

    end

    #####################################################
    #
    # resolved?
    #
    #####################################################
    def resolved?(d)

      @operands.each { |operand|
        return false if operand.resolved?(d)
      }
      return true

    end

    #####################################################
    #
    # query
    #
    #####################################################
    def query(d)

      queries=[]
      @operands.each { |operand|
        query=operand.query(d)
        queries += query
        return [{:dep=>"NOR", :msg=>"is not satisfied", :resolved=>false }, queries] if query.first[:resolved]
      }
      return [{:dep=>"NOR", :msg=>"is satisfied", :resolved=>true }, queries]

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

      @operands=operands

    end

    #####################################################
    #
    # resolved?
    #
    #####################################################
    def resolved?(d)

      ntrue=0
      @operands.each { |operand|
        ntrue += 1 if operand.resolved?(d)
        return false if ntrue > 1
      }
      return ntrue==1

    end

    #####################################################
    #
    # query
    #
    #####################################################
    def query(d)

      queries=[]
      ntrue=0
      @operands.each { |operand|
        query = operand.query(d)
        queries += query
        ntrue += 1 if query.first.resolved?(d)
        return [{:dep=>"XOR", :msg=>"is not satisfied", :resolved=>false }, queries] if ntrue > 1
      }
      if ntrue==1
        return [{:dep=>"XOR", :msg=>"is satisfied", :resolved=>true }, queries]
      else
        return [{:dep=>"XOR", :msg=>"is not satisfied", :resolved=>false }, queries]
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
    def initialize(operands,threshold=1.0)

      @operands=operands
      @threshold=threshold

    end

    #####################################################
    #
    # resolved?
    #
    #####################################################
    def resolved?(d)

      ntrue=0.0
      @operands.each { |operand|
        ntrue += 1.0 if operand.resolved?(d)
        return true if ntrue/@operands.size >= @threshold
      }
      return false

    end

    #####################################################
    #
    # query
    #
    #####################################################
    def query(d)

      queries=[]
      ntrue=0.0
      @operands.each { |operand|
        query = operand.query(d)
        queries += query
        ntrue += 1.0 if query.first[:resolved]
        return [{:dep=>"SOME", :msg=>"is satisfied", :resolved=>true }, queries] if ntrue/@operands.size >= @threshold

      }
      return [{:dep=>"SOME", :msg=>"is not satisfied", :resolved=>false }, queries]

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
    def initialize(task,state,cycle_offset)

      @task=task
      @state=state.upcase
      @cycle_offset=cycle_offset

    end

    #####################################################
    #
    # Resolved?
    #
    #####################################################
    def resolved?(d)

      return false if d.jobList.nil?      
      return false if d.jobList[@task].nil?
      return false if d.jobList[@task][d.cycle.getgm+@cycle_offset].nil?
      return d.jobList[@task][d.cycle.getgm+@cycle_offset].state==@state

    end

    #####################################################
    #
    # Query
    #
    #####################################################
    def query(d)

      return [{:dep=>"#{@task} of cycle #{(d.cycle.getgm+@cycle_offset).strftime("%Y%m%d%H%M")}", :msg=>"is not #{@state}", :resolved=>false }] if d.jobList.nil?
      return [{:dep=>"#{@task} of cycle #{(d.cycle.getgm+@cycle_offset).strftime("%Y%m%d%H%M")}", :msg=>"is not #{@state}", :resolved=>false }] if d.jobList[@task].nil?
      return [{:dep=>"#{@task} of cycle #{(d.cycle.getgm+@cycle_offset).strftime("%Y%m%d%H%M")}", :msg=>"is not #{@state}", :resolved=>false }] if d.jobList[@task][d.cycle.getgm+@cycle_offset].nil?
      if d.jobList[@task][d.cycle.getgm+@cycle_offset].state==@state
        return [{:dep=>"#{@task} of cycle #{(d.cycle.getgm+@cycle_offset).strftime("%Y%m%d%H%M")}", :msg=>"is #{@state}", :resolved=>true }]
      else
        return [{:dep=>"#{@task} of cycle #{(d.cycle.getgm+@cycle_offset).strftime("%Y%m%d%H%M")}", :msg=>"is not #{@state}", :resolved=>false }]
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
      @cycle_offset=cycle_offset
    end

    #####################################################
    #
    # Resolved?
    #
    #####################################################
    def resolved?(d)
      relcycle=d.cycle.getgm+@cycle_offset
      d.cycledefs.each do |cycledef|
        if cycledef.member?(relcycle)
              return true
        end
      end
      return false
    end

    #####################################################
    #
    # Query
    #
    #####################################################
    def query(d)
      relcycle=d.cycle.getgm+@cycle_offset
      d.cycledefs.each do |cycledef|
        if cycledef.member?(relcycle)
          return [{:dep=>"cycle #{relcycle.strftime("%Y%m%d%H%M")}", :msg=>"exists", :resolved=>true }]
        end
      end
      return [{:dep=>"cycle #{relcycle.strftime("%Y%m%d%H%M")}", :msg=>"does not exist", :resolved=>false }]
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

      @timestr=timestr

    end


    #####################################################
    #
    # Resolved?
    #
    #####################################################
    def resolved?(d)

      timestr=@timestr.to_s(d.cycle)
      t=Time.gm(timestr[0..3],
                timestr[4..5],
                timestr[6..7],
                timestr[8..9],
                timestr[10..11],
                timestr[12..13])

      return Time.now.getgm > t

     end    

    #####################################################
    #
    # Query
    #
    #####################################################
    def query(d)

      timestr=@timestr.to_s(d.cycle)
      t=Time.gm(timestr[0..3],
                timestr[4..5],
                timestr[6..7],
                timestr[8..9],
                timestr[10..11],
                timestr[12..13])
      
      if Time.now.getgm > t
        return [{:dep=>"Walltime", :msg=>"is > #{t}", :resolved=>true }]
      else
        return [{:dep=>"Walltime", :msg=>"is <= #{t}", :resolved=>false }]
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
    def initialize(datapath,age,minsize)

      @datapath=datapath
      @age=age
      @minsize=minsize
    
    end

    #####################################################
    #
    # Resolved?
    #
    #####################################################
    def resolved?(d)

      filename=@datapath.to_s(d.cycle)
      if d.workflowIOServer.exists?(filename)
        if d.workflowIOServer.size(filename) >= @minsize
          return Time.now > (d.workflowIOServer.mtime(filename) + @age)
        else
          return false
        end
      else
        return false
      end

    end

    #####################################################
    #
    # Query
    #
    #####################################################
    def query(d)

      filename=@datapath.to_s(d.cycle)
      if d.workflowIOServer.exists?(filename)
        if Time.now > (d.workflowIOServer.mtime(filename) + @age)
          if d.workflowIOServer.size(filename) >= @minsize
            return [{:dep=>filename, :msg=>"is available", :resolved=>true }]
          else
            return [{:dep=>filename, :msg=>"is not large enough", :resolved=>false }]
          end
        else
          return [{:dep=>filename, :msg=>"is not old enough", :resolved=>false }]
        end
      else
        return [{:dep=>filename, :msg=>"does not exist", :resolved=>false }]
      end

    end


  end

end
