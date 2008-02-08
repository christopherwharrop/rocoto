##########################################
#
# Add a succ method to class Time so that
# we can create a range of Time objects
# which are 1 day apart.
#
##########################################
class Time

  #####################################################
  #
  # succ - Returns a time one day from the current time
  #
  #####################################################
  def succ
    self+60*60*24
  end

end
