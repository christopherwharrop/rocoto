##########################################
#
# Module WorkflowMgr
#
##########################################
module WorkflowMgr

  require 'drb'
  require 'SystemTimer/system_timer'
  require 'socket'
  require 'tmpdir'
  require 'workflowmgr/utilities'

  ##########################################
  #
  # initialize
  #
  ##########################################
  def WorkflowMgr.launchServer(server)

    # Open a pipe for retrieving the URI and pid of the DRb server process
    rd,wr=IO.pipe

    # Get the pid of this process
    parent_pid=Process.pid

    # Fork a child process that will daemonize itself and start the server process
    child_pid = fork
    unless child_pid

      # Immediately close the read end of the pipe because the child is only going to write
      rd.close

      # This is the child process, so exec the server command
      # Pass the server the pid of the process that is launching it, the verbosity level, 
      # and the file descriptor to use for sending the URI back to the parent
      exec("#{server} #{parent_pid} #{WorkflowMgr::VERBOSE} #{wr.fileno}")

    end

    # Immediately close the write end of the pipe because we are only going to read
    wr.close

    # The server process will daemonize itself.  That will cause the
    # child process we just forked to exit.  We  must reap the status
    # of the child we just forked to avoid accumulcation of zombie processes.
    Process.waitpid(child_pid)

    # Initialize the URI and pid of the server process
    uri=""
    server_pid=0

    begin

      # Read the URI and pid of the server from the read end of the pipe we just created
      SystemTimer.timeout(10) do
        uri=rd.gets
        server_pid=rd.gets
        rd.close
      end

    rescue Timeout::Error
      
      # The URI file could not be read, so we cannot contact the server.
      # Either the server process died before it could write the URI and pid
      # or there was some sort of network problem.  The server process will 
      # shut itself down once it detects that this process has terminated.
      WorkflowMgr.log("Never received the URI and/or pid of #{File.basename(server)}.")
      raise "Never the received URI and/or pid of #{File.basename(server)}."

    end

    # Connect to the server process and return the object being served
    return [ DRbObject.new(nil,uri), Socket::getaddrinfo(Socket.gethostname, nil, nil, Socket::SOCK_STREAM)[0][3], server_pid ]

  end

end
