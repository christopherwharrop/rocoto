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

    # Fork a child process that will start the server process
    parent_pid=Process.ppid
    server_pid = fork
    if server_pid.nil?

      # This is the child process, so exec the server command
      # Tell the server the pid of the process that is launching it
      exec("#{server} #{parent_pid} #{WorkflowMgr::VERBOSE}")

    else     

      # The server process will daemonize itself.  That will cause the
      # child process we just forked to exit.  We  must harvest the status
      # of the child we just forked to avoid accumulcation of zombie processes.
      Process.waitpid(server_pid)

      # This is the parent process, so retrieve the URI of the forked server
      uri=""
      uri_file="#{Dir.tmpdir}/rocoto_uri_#{server_pid}"

      # Read URI of server from a file in /tmp
      begin

        # Wait for the uri file to become available, then read it
        SystemTimer.timeout(10) do
          while !File.exists?(uri_file) do
            sleep 0.25 
          end
          file=File.new(uri_file)
          uri=file.gets
          while uri.nil? do
            sleep 0.25
            uri=file.gets
          end
          file.close
          
        end

      rescue Timeout::Error
      
        # The uri file could not be found, so we cannot contact the server.
	# Either it died before it could write the URI file, or the file disappeared
        # The server process will shut itself down once it detects that this process
        # has terminated.
        WorkflowMgr.log("Could not find URI of #{File.basename(server)} in #{uri_file}.  Either the server crashed at startup, or the #{Dir.tmpdir} filesystem is misbehaving.")
        raise "Could not find URI of #{File.basename(server)} in #{uri_file}"

      ensure

        # Make sure to remove the temporary URI file after we have read it
        File.delete(uri_file) unless !File.exists?(uri_file)

      end

      # Connect to the server process and return the object being served
      return [ DRbObject.new(nil,uri), Socket::getaddrinfo(Socket.gethostname, nil, nil, Socket::SOCK_STREAM)[0][3], server_pid ]

    end

  end

end
