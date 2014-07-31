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
    def resolved?(cycle,jobList,workflowIOServer,cycledefs)

      begin
        return(@root.resolved?(cycle,jobList,workflowIOServer,cycledefs))
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
    def query(cycle,jobList,workflowIOServer,cycledefs)

      begin
        return(@root.query(cycle,jobList,workflowIOServer,cycledefs))
      rescue WorkflowIOHang
        WorkflowMgr.stderr("#{$!}",2)
        WorkflowMgr.log("#{$!}")
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
    def resolved?(cycle,jobList,workflowIOServer,cycledefs)

      return !@operand.resolved?(cycle,jobList,workflowIOServer,cycledefs)

    end

    #####################################################
    #
    # query
    #
    #####################################################
    def query(cycle,jobList,workflowIOServer,cycledefs)
      query=@operand.query(cycle,jobList,workflowIOServer,cycledefs)
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
    def resolved?(cycle,jobList,workflowIOServer,cycledefs)

      @operands.each { |operand|
        return false unless operand.resolved?(cycle,jobList,workflowIOServer,cycledefs)
      }
      return true

    end


    #####################################################
    #
    # query
    #
    #####################################################
    def query(cycle,jobList,workflowIOServer,cycledefs)

      queries=[]
      @operands.each { |operand|
        query = operand.query(cycle,jobList,workflowIOServer,cycledefs)
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
    def resolved?(cycle,jobList,workflowIOServer,cycledefs)

      @operands.each { |operand|
        return true if operand.resolved?(cycle,jobList,workflowIOServer,cycledefs)
      }
      return false

    end


    #####################################################
    #
    # query
    #
    #####################################################
    def query(cycle,jobList,workflowIOServer,cycledefs)

      queries=[]
      @operands.each { |operand|
        query = operand.query(cycle,jobList,workflowIOServer,cycledefs)
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
    def resolved?(cycle,jobList,workflowIOServer,cycledefs)

      @operands.each { |operand|
        return true if !operand.resolved?(cycle,jobList,workflowIOServer,cycledefs)
      }
      return false

    end

    #####################################################
    #
    # query
    #
    #####################################################
    def query(cycle,jobList,workflowIOServer,cycledefs)

      queries=[]
      @operands.each { |operand|
        query = operand.query(cycle,jobList,workflowIOServer,cycledefs)
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
    def resolved?(cycle,jobList,workflowIOServer,cycledefs)

      @operands.each { |operand|
        return false if operand.resolved?(cycle,jobList,workflowIOServer,cycledefs)
      }
      return true

    end

    #####################################################
    #
    # query
    #
    #####################################################
    def query(cycle,jobList,workflowIOServer,cycledefs)

      queries=[]
      @operands.each { |operand|
        query=operand.query(cycle,jobList,workflowIOServer,cycledefs)
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
    def resolved?(cycle,jobList,workflowIOServer,cycledefs)

      ntrue=0
      @operands.each { |operand|
        ntrue += 1 if operand.resolved?(cycle,jobList,workflowIOServer,cycledefs)
        return false if ntrue > 1
      }
      return ntrue==1

    end

    #####################################################
    #
    # query
    #
    #####################################################
    def query(cycle,jobList,workflowIOServer,cycledefs)

      queries=[]
      ntrue=0
      @operands.each { |operand|
        query = operand.query(cycle,jobList,workflowIOServer,cycledefs)
        queries += query
        ntrue += 1 if query.first.resolved?
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
    def resolved?(cycle,jobList,workflowIOServer,cycledefs)

      ntrue=0.0
      @operands.each { |operand|
        ntrue += 1.0 if operand.resolved?(cycle,jobList,workflowIOServer,cycledefs)
        return true if ntrue/@operands.size >= @threshold
      }
      return false

    end

    #####################################################
    #
    # query
    #
    #####################################################
    def query(cycle,jobList,workflowIOServer,cycldefs)

      queries=[]
      ntrue=0.0
      @operands.each { |operand|
        query = operand.query(cycle,jobList,workflowIOServer,cycledefs)
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
    def resolved?(cycle,jobList,workflowIOServer,cycledefs)

      return false if jobList.nil?      
      return false if jobList[@task].nil?
      return false if jobList[@task][cycle.getgm+@cycle_offset].nil?
      return jobList[@task][cycle.getgm+@cycle_offset].state==@state

    end

    #####################################################
    #
    # Query
    #
    #####################################################
    def query(cycle,jobList,workflowIOServer,cycledefs)

      return [{:dep=>"#{@task} of cycle #{(cycle.getgm+@cycle_offset).strftime("%Y%m%d%H%M")}", :msg=>"is not #{@state}", :resolved=>false }] if jobList.nil?
      return [{:dep=>"#{@task} of cycle #{(cycle.getgm+@cycle_offset).strftime("%Y%m%d%H%M")}", :msg=>"is not #{@state}", :resolved=>false }] if jobList[@task].nil?
      return [{:dep=>"#{@task} of cycle #{(cycle.getgm+@cycle_offset).strftime("%Y%m%d%H%M")}", :msg=>"is not #{@state}", :resolved=>false }] if jobList[@task][cycle.getgm+@cycle_offset].nil?
      if jobList[@task][cycle.getgm+@cycle_offset].state==@state
        return [{:dep=>"#{@task} of cycle #{(cycle.getgm+@cycle_offset).strftime("%Y%m%d%H%M")}", :msg=>"is #{@state}", :resolved=>true }]
      else
        return [{:dep=>"#{@task} of cycle #{(cycle.getgm+@cycle_offset).strftime("%Y%m%d%H%M")}", :msg=>"is not #{@state}", :resolved=>false }]
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
    def resolved?(cycle,jobList,workflowIOServer,cycledefs)
      relcycle=cycle.getgm+@cycle_offset
      cycledefs.each do |cycledef|
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
    def query(cycle,jobList,workflowIOServer,cycledefs)
      relcycle=cycle.getgm+@cycle_offset
      cycledefs.each do |cycledef|
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
    def resolved?(cycle,jobList,workflowIOServer,cycledefs)

      timestr=@timestr.to_s(cycle)
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
    def query(cycle,jobList,workflowIOServer,cycledefs)

      timestr=@timestr.to_s(cycle)
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
    def resolved?(cycle,jobList,workflowIOServer,cycledefs)

      filename=@datapath.to_s(cycle)
      if workflowIOServer.exists?(filename)
        if workflowIOServer.size(filename) >= @minsize
          return Time.now > (workflowIOServer.mtime(filename) + @age)
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
    def query(cycle,jobList,workflowIOServer,cycledefs)

      filename=@datapath.to_s(cycle)
      if workflowIOServer.exists?(filename)
        if Time.now > (workflowIOServer.mtime(filename) + @age)
          if workflowIOServer.size(filename) >= @minsize
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
