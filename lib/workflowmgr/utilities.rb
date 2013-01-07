##########################################
#
# Module WorkflowMgr
#
##########################################
module WorkflowMgr

  require 'system_timer'

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

    if OPTIONS.verbose >= level
     STDERR.puts "#{Time.now.strftime("%x %X %Z")} :: #{message}"
    end

  end

  ##########################################  
  #
  # WorkflowMgr.log
  #
  ##########################################
  def WorkflowMgr.log(message)

    File.open("#{ENV['HOME']}/.rocoto/log","a") { |f|
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

end  # module workflowmgr
