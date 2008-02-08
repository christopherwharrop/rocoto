unless defined? $__environment__

##########################################
#
# Class Environment
#
##########################################
class Environment
 
  attr_reader :name

  #####################################################
  #
  # initialize
  #
  #####################################################
  def initialize(name,value)

    @name=name
    @value=value

  end

  #####################################################
  #
  # get_value
  #
  #####################################################
  def value(time)

    return @value.to_s(time)

  end
  

end

$__environment__ == __FILE__
end
