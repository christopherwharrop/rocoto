unless defined? $__property__

##########################################
#
# Class Property
#
##########################################
class Property

  attr_reader :name
 
  #####################################################
  #
  # initialize
  #
  #####################################################
  def initialize(name,value=nil)

    @name=name
    @value=value

  end


  #####################################################
  #
  # get_value
  #
  #####################################################
  def value(time)

    if @value.nil?
      return ""
    else
      return @value.to_s(time)
    end

  end


end

$__property__ == __FILE__
end
