unless defined? $__dependency__

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
  def resolved?(cycleTime)

    return(@root.resolved?(cycleTime))

  end

end


##########################################
#
# Class Dependency_Operator
#
##########################################
class Dependency_Operator

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
  def resolved?(cycleTime)

  end

end


##########################################
#
# Class Dependency_NOT_Operator
#
##########################################
class Dependency_NOT_Operator < Dependency_Operator

  #####################################################
  #
  # resolved?
  #
  #####################################################
  def resolved?(cycleTime)

    return !@operands[0].resolved?(cycleTime)

  end

end


##########################################
#
# Class Dependency_AND_Operator
#
##########################################
class Dependency_AND_Operator < Dependency_Operator

  #####################################################
  #
  # initialize
  #
  #####################################################
  def initialize(operands,max_missing=0)

    @operands=operands
    @max_missing=max_missing.to_i

  end

  #####################################################
  #
  # resolved?
  #
  #####################################################
  def resolved?(cycleTime)

    missing=0
    @operands.each { |operand|
      unless operand.resolved?(cycleTime)
        missing+=1
      end
      return false if missing > @max_missing
    }
    return true

  end

end


##########################################
#
# Class Dependency_OR_Operator
#
##########################################
class Dependency_OR_Operator < Dependency_Operator

  #####################################################
  #
  # resolved?
  #
  #####################################################
  def resolved?(cycleTime)

    @operands.each { |operand|
      return true if operand.resolved?(cycleTime)      
    }
    return false

  end

end


##########################################
#
# Class Dependency_Operand
#
##########################################
class Dependency_Operand

  #####################################################
  #
  # initialize
  #
  #####################################################
  def initialize(trigger)

    @trigger=trigger

  end

  #####################################################
  #
  # Resolved?
  #
  #####################################################
  def resolved?(cycleTime)

  end

end


##########################################
#
# Class TaskDoneOkayDependency
#
##########################################
class TaskDoneOkayDependency < Dependency_Operand

  #####################################################
  #
  # initialize
  #
  #####################################################
  def initialize(trigger,cycle=0)

    super(trigger)
    @cycle=cycle

  end

  #####################################################
  #
  # Resolved?
  #
  #####################################################
  def resolved?(cycleTime)

    return @trigger.done_okay?(cycleTime+@cycle)

  end

end

##########################################
#
# Class TimeDependency
#
##########################################
class TimeDependency < Dependency_Operand

  #####################################################
  #
  # Resolved?
  #
  #####################################################
  def resolved?(cycleTime)

    timestr=@trigger.to_s(cycleTime)
    return Time.now > Time.gm(timestr[0..3],
                              timestr[4..5],
                              timestr[6..7],
                              timestr[8..9],
                              timestr[10..11],
                              timestr[12..13])                  

  end

end

##########################################
#
# Class FileAgeDependency
#
##########################################
class FileDependency < Dependency_Operand

  #####################################################
  #
  # initialize
  #
  #####################################################
  def initialize(trigger,age=300)

    super(trigger)
    @age=age

  end

  #####################################################
  #
  # Resolved?
  #
  #####################################################
  def resolved?(cycleTime)

    filename=@trigger.to_s(cycleTime)
    if File.exists?(filename)
      return Time.now > (File.mtime(filename) + @age)
    else
      return false
    end

  end

end

$__dependency__ == __FILE__
end
