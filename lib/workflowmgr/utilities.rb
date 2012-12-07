##########################################
#
# Module WorkflowMgr
#
##########################################
module WorkflowMgr

  ##########################################
  #
  # Class WorkflowIOHang
  #
  ##########################################
  class WorkflowIOHang < RuntimeError
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
  # WorkflowMgr.log
  #
  ##########################################
  def WorkflowMgr.log(message)

    File.open("#{ENV['HOME']}/.rocoto/log","a") { |f|
      f.puts "#{Time.now.strftime("%x %X %Z")} :: #{message}"
    }

  end

end  # module workflowmgr
