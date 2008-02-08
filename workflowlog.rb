unless defined? $__workflowlog__

##########################################
#
# WorkflowLog
#
##########################################
class WorkflowLog


  #####################################################
  #
  # initialize
  #
  #####################################################
  def initialize(logfile)

    @name=logfile

  end

  #####################################################
  #
  # log
  #
  #####################################################
  def log(cycle,msg)

    host=`hostname -s`.chomp
    logfile=File.new(@name.to_s(cycle),"a")
    logfile.puts("#{Time.now} :: #{host} :: #{msg}")
    logfile.close

  end

end

$__workflowlog__ == __FILE__
end
