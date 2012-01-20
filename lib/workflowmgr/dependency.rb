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
    def resolved?(cycle,jobList,fileStatServer)

      return(@root.resolved?(cycle,jobList,fileStatServer))

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

      @operand=operand

    end

    #####################################################
    #
    # resolved?
    #
    #####################################################
    def resolved?(cycle,jobList,fileStatServer)

      return !@operand.resolved?(cycle,jobList,fileStatServer)

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
    def resolved?(cycle,jobList,fileStatServer)

      @operands.each { |operand|
        return false unless operand.resolved?(cycle,jobList,fileStatServer)
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
    def resolved?(cycle,jobList,fileStatServer)

      @operands.each { |operand|
        return true if operand.resolved?(cycle,jobList,fileStatServer)
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
    def resolved?(cycle,jobList,fileStatServer)

      @operands.each { |operand|
        return true if !operand.resolved?(cycle,jobList,fileStatServer)
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
    def resolved?(cycle,jobList,fileStatServer)

      @operands.each { |operand|
        return false if operand.resolved?(cycle,jobList,fileStatServer)
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
    def resolved?(cycle,jobList,fileStatServer)

      ntrue=0
      @operands.each { |operand|
        ntrue += 1 if operand.resolved?(cycle,jobList,fileStatServer)
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
    def resolved?(cycle,jobList,fileStatServer)

      ntrue=0.0
      @operands.each { |operand|
        ntrue += 1.0 if operand.resolved?(cycle,jobList,fileStatServer)
        return true if ntrue/@operands.size >= threshold
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
    def initialize(task,status,cycle_offset)

      @task=task
      @status=status
      @cycle_offset=cycle_offset

    end

    #####################################################
    #
    # Resolved?
    #
    #####################################################
    def resolved?(cycle,jobList,fileStatServer)

      return false if jobList.nil?      
      return false if jobList[@task].nil?
      return false if jobList[@task][cycle.getgm+@cycle_offset].nil?
      return jobList[@task][cycle.getgm+@cycle_offset][:state]==@status

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
    def resolved?(cycle,jobList,fileStatServer)

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
    def resolved?(cycle,jobList,fileStatServer)

      filename=@datapath.to_s(cycle)
      if fileStatServer.exists?(filename)
        return Time.now > (fileStatServer.mtime(filename) + @age)
      else
        return false
      end

    end

  end

end
