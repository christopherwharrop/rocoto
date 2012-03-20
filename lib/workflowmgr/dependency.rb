##########################################
#
# Module WorkflowMgr
#
##########################################
module WorkflowMgr

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
    def resolved?(cycle,jobList,workflowIOServer)

      begin
        return(@root.resolved?(cycle,jobList,workflowIOServer))
      rescue WorkflowIOHang
        puts $!
        return false
      end

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
    def resolved?(cycle,jobList,workflowIOServer)

      return !@operand.resolved?(cycle,jobList,workflowIOServer)

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
    def resolved?(cycle,jobList,workflowIOServer)

      @operands.each { |operand|
        return false unless operand.resolved?(cycle,jobList,workflowIOServer)
      }
      return true

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
    def resolved?(cycle,jobList,workflowIOServer)

      @operands.each { |operand|
        return true if operand.resolved?(cycle,jobList,workflowIOServer)
      }
      return false

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
    def resolved?(cycle,jobList,workflowIOServer)

      @operands.each { |operand|
        return true if !operand.resolved?(cycle,jobList,workflowIOServer)
      }
      return false

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
    def resolved?(cycle,jobList,workflowIOServer)

      @operands.each { |operand|
        return false if operand.resolved?(cycle,jobList,workflowIOServer)
      }
      return true

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
    def resolved?(cycle,jobList,workflowIOServer)

      ntrue=0
      @operands.each { |operand|
        ntrue += 1 if operand.resolved?(cycle,jobList,workflowIOServer)
        return false if ntrue > 1
      }
      return ntrue==1

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
    def resolved?(cycle,jobList,workflowIOServer)

      ntrue=0.0
      @operands.each { |operand|
        ntrue += 1.0 if operand.resolved?(cycle,jobList,workflowIOServer)
        return true if ntrue/@operands.size >= @threshold
      }
      return false

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
    def resolved?(cycle,jobList,workflowIOServer)

      return false if jobList.nil?      
      return false if jobList[@task].nil?
      return false if jobList[@task][cycle.getgm+@cycle_offset].nil?
      return jobList[@task][cycle.getgm+@cycle_offset][:state]==@state

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
    def resolved?(cycle,jobList,workflowIOServer)

      t=@timestr.to_s(cycle)      
      return Time.now.getgm > Time.gm(t[0..3],
                                      t[4..5],
                                      t[6..7],
                                      t[8..9],
                                      t[10..11],
                                      t[12..13])
    
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
    def initialize(datapath,age)

      @datapath=datapath
      @age=age
    
    end

    #####################################################
    #
    # Resolved?
    #
    #####################################################
    def resolved?(cycle,jobList,workflowIOServer)

      filename=@datapath.to_s(cycle)
      if workflowIOServer.exists?(filename)
        return Time.now > (workflowIOServer.mtime(filename) + @age)
      else
        return false
      end

    end

  end

end
