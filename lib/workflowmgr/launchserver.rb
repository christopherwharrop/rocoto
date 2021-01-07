##########################################
#
# Module WorkflowMgr
#
##########################################
module WorkflowMgr

  require 'drb'
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
      if RUBY_VERSION < "1.9.0"
        exec("#{server} #{parent_pid} #{WorkflowMgr::VERBOSE} #{WorkflowMgr::WORKFLOW_ID} #{wr.fileno}")
      else
        # Make sure wr file descriptor is inherited by child processes
        exec("#{server} #{parent_pid} #{WorkflowMgr::VERBOSE} #{WorkflowMgr::WORKFLOW_ID} #{wr.fileno}", {wr=>wr})
      end

    end

    # Immediately close the write end of the pipe because we are only going to read
    wr.close

    # The server process will daemonize itself.  That will cause the
    # child process we just forked to exit.  We  must reap the status
    # of the child we just forked to avoid accumulcation of zombie processes.
    Process.waitpid(child_pid)

    # Initialize the URI and pid of the server process
    uri_str=""
    encoded_uri=""
    server_pid=0

    begin

      # Read the URI and pid of the server from the read end of the pipe we just created
      WorkflowMgr.timeout(10) do
        uri_str=rd.gets
        uri_str.chomp! unless uri_str.nil?
        server_pid=rd.gets
        server_pid.chomp! unless server_pid.nil?
        rd.close
      end

    rescue Timeout::Error

      # The URI file could not be read, so we cannot contact the server.
      # Either the server process died before it could write the URI and pid
      # or there was some sort of network problem.  The server process will
      # shut itself down once it detects that this process has terminated.
      WorkflowMgr.log("Never received the URI and/or pid of #{File.basename(server)}.")
      raise "Never received the URI and/or pid of #{File.basename(server)}."

    end

    # Connect to the server process and return the object being served
    encoded_uri=uri_str.encode("UTF-8", "binary", invalid: :replace, undef: :replace, replace: '')
    return [ DRbObject.new(nil,encoded_uri), Socket::getaddrinfo(Socket.gethostname, nil, nil, Socket::SOCK_STREAM)[0][3], server_pid ]

  end

end
