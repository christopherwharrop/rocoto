##########################################
#
# Module WorkflowMgr
#
##########################################
module WorkflowMgr

  require 'drb'
  require 'timeout'

  ##########################################
  #
  # initialize
  #
  ##########################################
  def WorkflowMgr.launchServer(server)


    # Fork a child process
    server_pid = fork

    if server_pid.nil?

      # This is the child process, so exec the server command
      exec(server)        

    else

      # This is the parent process, so retrieve the URI of the forked server
      uri=""
      uri_file="/tmp/workflowmgr_#{server_pid}_uri"

      # Read URI of server from a file in /tmp
      begin

        # Wait for the uri file to become available, then read it
        Timeout::timeout(2) do
          while !File.exists?(uri_file) do
            sleep 0.25 
          end
          uri=IO.readlines(uri_file).join
        end

      rescue Timeout::Error
      
        # The uri file could not be found, so we cannot contact the server
	# Either it died before it could write the URI file, or the file disappeared
        # Attempt to kill the server process in case it is running
        Process.kill(:TERM,server_pid)
        raise "Could not find address of server in #{uri_file}"

      ensure

        # Make sure to remove the temporary URI file after we have read it
        File.delete(uri_file) unless !File.exists?(uri_file)

      end

      # Connect to the server process and return the object being served
      return DRbObject.new(nil,uri)

    end

  end

end
