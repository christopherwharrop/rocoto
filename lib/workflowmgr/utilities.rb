##########################################
#
# Module WorkflowMgr
#
##########################################
module WorkflowMgr

  if RUBY_VERSION < "1.9.0"
    require 'system_timer'
  else
    require 'timeout'
  end
  require 'open4'

  ##########################################
  #
  # Class WorkflowIOHang
  #
  ##########################################
  class WorkflowIOHang < RuntimeError
  end


  ##########################################
  #
  # Class SchedulerDown
  #
  ##########################################
  class SchedulerDown < RuntimeError
  end


  ##########################################  
  #
  # WorkflowMgr.version
  #
  ##########################################
  def WorkflowMgr.version

    IO.readlines("#{File.expand_path('../../../',__FILE__)}/VERSION",nil)[0]

  end


  ##########################################  
  #
  # WorkflowMgr.timeout
  #
  ##########################################
  def WorkflowMgr.timeout(s)

    if RUBY_VERSION < "1.9.0"
      SystemTimer.timeout(s) do
        yield
      end
    else
      Timeout::timeout(s) do
        yield
      end
    end

  end


  ##########################################
  #
  # WorkflowMgr.ddhhmmss_to_seconds
  #
  ##########################################
  def WorkflowMgr.ddhhmmss_to_seconds(ddhhmmss)

    secs=0
    unless ddhhmmss.nil?
      sign=ddhhmmss[/^-/].nil? ? 1 : -1
      ddhhmmss.split(":").reverse.each_with_index {|i,index|
        if index==3
          secs+=i.to_i.abs*3600*24
        elsif index < 3
          secs+=i.to_i.abs*60**index
        else
          raise "Invalid dd:hh:mm:ss, '#{ddhhmmss}'"
        end
      }
      secs*=sign
    end
    return secs

  end


  ##########################################  
  #
  # WorkflowMgr.seconds_to_hhmmss
  #
  ##########################################
  def WorkflowMgr.seconds_to_hhmmss(seconds)

    s=seconds
    hours=(s / 3600.0).floor
    s -= hours * 3600
    minutes=(s / 60.0).floor
    s -= minutes * 60
    seconds=s

    hhmmss=sprintf("%0d:%02d:%02d",hours,minutes,seconds)
    return hhmmss

  end

  ##########################################  
  #
  # WorkflowMgr.seconds_to_hhmm
  #
  ##########################################
  def WorkflowMgr.seconds_to_hhmm(seconds)

    s=seconds
    hours=(s / 3600.0).floor
    s -= hours * 3600
    minutes=(s / 60.0).ceil
    if minutes > 59
      hours += 1
      minutes = 0
    end

    hhmm=sprintf("%0d:%02d",hours,minutes)
    return hhmm

  end


  ##########################################  
  #
  # WorkflowMgr.config_set
  #
  ##########################################
  def WorkflowMgr.config_set(config)

    WorkflowMgr.const_set("CONFIG",config)    

  end


  ##########################################  
  #
  # WorkflowMgr.options_set
  #
  ##########################################
  def WorkflowMgr.options_set(options)

    WorkflowMgr.const_set("OPTIONS",options)

  end


  ##########################################  
  #
  # WorkflowMgr.stderr
  #
  ##########################################
  def WorkflowMgr.stderr(message,level=0)

    return if message.nil?
    return if message.empty?

    if VERBOSE >= level
     STDERR.puts "#{Time.now.strftime("%x %X %Z")} :: #{message}"
    end

  end

  ##########################################  
  #
  # WorkflowMgr.log
  #
  ##########################################
  def WorkflowMgr.log(message)

    return if message.nil?
    return if message.empty?

    rocotolog="#{ENV['HOME']}/.rocoto/log"

    if File.exists?(rocotolog)
      # Rotate log if necessary
      log_mod_time = File.mtime(rocotolog)
      if log_mod_time.day != Time.now.day
        FileUtils.mv(rocotolog,rocotolog+".#{log_mod_time.strftime('%Y%m%d')}")
      end
      
      # Get the max age (in days) of the log file from the configuration
      # NOTE: This is a hack due to poor design preventing proper access to the configuration object
      maxAge = YAML.load_file("#{ENV['HOME']}/.rocoto/rocotorc")[:MaxLogDays]
      
      # Remove files last modified more than MaxAge days ago
      Dir[rocotolog+".[0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9]"].each { |logfile| 
        if (Time.now - File.mtime(logfile)) > (maxAge * 24 * 3600)
          FileUtils.rm_f(logfile)
        end
      }
    end

    # Log the message
    File.open(rocotolog,"a") { |f|
      f.puts "#{Time.now.strftime("%x %X %Z")} :: #{message}"
    }

  end


  ##########################################  
  #
  # WorkflowMgr.run
  #
  ##########################################
  def WorkflowMgr.run(command,timeout=30)

    begin
      pipe = IO.popen(command)
    rescue Exception
      WorkflowMgr.log("WARNING! Could not run'#{command}': #{$!}")
      raise "Execution of command #{command} unsuccessful"
    end

    output = ""
    exit_status=0
    begin
      SystemTimer.timeout(timeout) do
        while (!pipe.eof?)  do
          output += pipe.gets(nil)
        end
        status=Process.waitpid2(pipe.pid)
        exit_status=status[1].exitstatus
      end
    rescue Timeout::Error
      Process.kill('KILL', pipe.pid)
      pipe.close
      WorkflowMgr.log("WARNING! The command '#{command}' timed out after #{timeout} seconds.")
      raise Timeout::Error
    end
    pipe.close
    [output,exit_status]

  end


  ##########################################  
  #
  # WorkflowMgr.run4
  #
  ##########################################
  def WorkflowMgr.run4(command,timeout=30)

    # Turn off garbage collection to avoid possible seg faults caused by Ruby 
    # trying to allocate objects during GC when calling IO.select and IO#read.
    # See: http://tickets.opscode.com/browse/CHEF-2916
    GC.disable

    begin
      pid, stdin, stdout, stderr = Open4::popen4(command)
      stdin.close
    rescue Exception
      raise "Execution of '#{command}' unsuccessful: #{$!}"
    end

    error = ""
    output = ""
    exit_status=0
    begin
      WorkflowMgr.timeout(timeout) do
        while (!stdout.eof?)  do
          output += stdout.gets(nil)
        end
        stdout.close

        while (!stderr.eof?)  do
          error += stderr.gets(nil)
        end
        stderr.close

        status=Process.waitpid2(pid)
        exit_status=status[1].exitstatus
      end
    rescue Timeout::Error
      Process.kill('KILL', pid)
      stdout.close
      stderr.close
      WorkflowMgr.log("WARNING! The command '#{command}' timed out after #{timeout} seconds.")
      raise Timeout::Error
    end
    [output,error,exit_status]

  ensure

  end


end  # module workflowmgr
