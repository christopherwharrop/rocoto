##########################################
#
# Module WorkflowMgr
#
##########################################
module WorkflowMgr

  ##########################################  
  #
  # Class ForkitTimeoutException
  #
  ##########################################
  class ForkitTimeoutException < RuntimeError
  end

  ##########################################  
  #
  # WorkflowMgr.forkit
  #
  ##########################################
  def WorkflowMgr.forkit(timelimit,&fblock)

    require 'system_timer'

    # Create a pair of pipe endpoints
    pread,pwrite=IO.pipe

    # Fork a child process to run the block
    pid=Process.fork do

      begin

        # Close the read end of the pipe in the child
        pread.close

        # Attempt to run the block
        result=fblock.call

        # Write the result of the block to the write end of the pipe
        pwrite.write Marshal.dump(result)

        # Close the write end of the pipe
        pwrite.close

      rescue

        # Write the exception to the write end of the pipe
        pwrite.write Marshal.dump($!)

        # Close the write end of the pipe
        pwrite.close

      end

    end

    # Wait for the block to run in the child process
    begin
 
      # Timeout after timelimit seconds
      SystemTimer.timeout(timelimit) do

        # Close the write end of the pipe
        pwrite.close

        # Read the result of the block from the pipe
        result=Marshal.load(pread.read)

        # Close the read end of the pipe
        pread.close

        # Wait for the child process to quit
        Process.waitpid(pid)

        # Return the result of the block
        if result.is_a?(Exception)
          raise result
        else
          return result
        end

      end

    rescue Timeout::Error

      # Kill the block
      Process.kill(:KILL,pid)    

      # The block took too long, exit with an error
      raise WorkflowMgr::ForkitTimeoutException, "The block timed out"
    end

  end  # def forkit

  ##########################################
  #
  # WorkflowMgr.ioerr
  #
  ##########################################
  def WorkflowMgr.ioerr(path)

    STDERR.puts
    STDERR.puts "ERROR: An I/O operation timed out while reading, writing, or testing for the existence of '#{path}'"
    STDERR.puts "       The filesystem containing '#{path}' may be offline."
    STDERR.puts

  end

end  # module workflowmgr
