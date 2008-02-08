unless defined? $__command__

require 'timeout'

##########################################################
#
# Class Command
#
##########################################################
class Command

##########################################################  
#
# run
#
##########################################################
def Command.run(cmd,max_time=60)

  output=""
  pipe=IO.popen("#{cmd} 2>&1","w+")
  if pipe
    begin
      timeout(max_time) do
        while !pipe.eof? do
          output+=pipe.gets
        end
      end
    rescue TimeoutError
      raise "'#{cmd}' timed out!\n\n#{output[0]}" 
    end
    Process.wait(pipe.pid)
    if $?.signaled?
      exit_status=$?.termsig
    elsif $?.exited?
      exit_status=$?.exitstatus
    end
  else
    raise "ERROR! Could not open pipe to #{cmd}"
  end
  return [output,exit_status]

end

end   # Class Command

$__command__ == __FILE__
end
