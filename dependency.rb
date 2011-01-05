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
    @max_missing=0 if @max_missing.nil? 
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

    resolved=@trigger.done_okay?(cycleTime+@cycle)
    if resolved
      Debug::message("    Dependency on task '#{@trigger.name}' is satisfied",10)
    else
      Debug::message("    Dependency on task '#{@trigger.name}' is not satisfied",10)
    end
    return resolved

  end

end

##########################################
#
# Class TaskDoneDependency
#
##########################################
class TaskDoneDependency < Dependency_Operand

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

    resolved=@trigger.done?(cycleTime+@cycle)
    if resolved
      Debug::message("    Dependency on task '#{@trigger.name}' is satisfied",10)
    else
      Debug::message("    Dependency on task '#{@trigger.name}' is not satisfied",10)
    end
    return resolved

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
    resolved= Time.now.getutc > Time.gm(timestr[0..3],
                                        timestr[4..5],
                                        timestr[6..7],
                                        timestr[8..9],
                                        timestr[10..11],
                                        timestr[12..13])                  
    if resolved
      Debug::message("    Dependency on time '#{@trigger.to_s(cycleTime)}' is satisfied",10)
    else
      Debug::message("    Dependency on time '#{@trigger.to_s(cycleTime)}' is not satisfied",10)
    end
    return resolved
    
  end

end

##########################################
#
# Class FileAgeDependency
#
##########################################
class FileDependency < Dependency_Operand

  require 'timeout'

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

    begin
      status = Timeout::timeout(5) {
        if File.exists?(filename)
          resolved=Time.now > (File.mtime(filename) + @age)
          if resolved
            Debug::message("    Dependency on file '#{@trigger.to_s(cycleTime)}' is satisfied",10)
          else
            Debug::message("    Dependency on file '#{@trigger.to_s(cycleTime)}' is not satisfied (file is not old enough)",10)
          end
          return resolved
        else
          Debug::message("    Dependency on file '#{@trigger.to_s(cycleTime)}' is not satisfied (file does not exist)",10)
          return false
        end
      }
    rescue Timeout::Error
      Debug::message("    Dependency on file '#{@trigger.to_s(cycleTime)}' is not satisfied (filesystem may be slow or hung)",10)
      return false
    end

  end

end

$__dependency__ == __FILE__
end
