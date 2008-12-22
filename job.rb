unless defined? $__job__

##########################################
#
# Class Job
#
##########################################
class Job

  attr_reader :state
  attr_reader :id
  attr_reader :exit_status
  attr_reader :execution_time

  #####################################################
  #
  # initialize
  #
  #####################################################
  def initialize(command,scheduler,attributes)

    begin

      @command=command
      @scheduler=scheduler
      @attributes=attributes

      @id=nil
      @state="new"
      @exit_status=nil
      @execution_time=nil

    rescue
      raise $!

    end

  end

  #####################################################
  #
  # submit
  #
  #####################################################
  def submit
    
    begin

      @id=@scheduler.submit(@command,@attributes)
      @state="new"
      @exit_status=nil
      @execution_time=nil
      @unknown_count=0

    rescue 
      raise "ERROR: Submission of '#{@command}' failed.  #{$!}"
    end

  end

  #####################################################
  #
  # qdel
  #
  #####################################################
  def qdel
    
    begin

      return if @id.nil?
      result=@scheduler.qdel(@id)
      if result != 0
        puts "Attempt to delete job #{@id} failed"
      else
        puts "Job #{@id} has been deleted"
      end

    rescue 
      puts "Error! qdel failed:\n\n#{$!}"
    end

  end

  #####################################################
  #
  # update_state
  #
  #####################################################
  def update_state

    # Don't update if the job hasn't been submitted yet
    return if @id.nil?

    # Don't update if we've already got the exit status
    return unless @exit_status.nil?

    # Get the latest state if it's not already done
    unless @state=="done"
      begin
        @state=@scheduler.get_job_state(@id)
      rescue
        @state="unknown"
        raise $!
      end
    end

    # Get the exit status if it's done
    if @state=="done"
      @exit_record=@scheduler.get_job_exit_record(@id)     
      if @exit_record.nil?
        @unknown_count=@unknown_count+1
        @state="unknown"
        if @unknown_count > 3
          puts "ERROR! Could not find accounting record for job #{@id} too many times, must assume it has crashed"
          @state="done"
          @exit_status=255
        end
      else
        @exit_status=@exit_record['exit_status']
        if @exit_record['end_time'].nil? || @exit_record['start_time'].nil?
          @execution_time=0
        else
          @execution_time=@exit_record['end_time'] - @exit_record['start_time']
        end
        if @unknown_count > 1
          puts "Warning!  Found record for job #{@id} that was previously missing #{@unknown_count} times in a row"
        end
        @unknown_count=0
      end    
    end

  end

  #####################################################
  #
  # running?
  #
  #####################################################
  def running?

    return (@state=="r" || @state=="t")

  end

  #####################################################
  #
  # waiting?
  #
  #####################################################
  def waiting?

    return (@state=="qw")

  end

  #####################################################
  #
  # error_state?
  #
  #####################################################
  def error_state?

    return (@state=="Eqw")

  end

  #####################################################
  #
  # done?
  #
  #####################################################
  def done?

    return (@state=="done" || error_state?)

  end

  #####################################################
  #
  # done_okay?
  #
  #####################################################
  def done_okay?

    return (@state=="done" && @exit_status==0)

  end

  #####################################################
  #
  # crashed?
  #
  #####################################################
  def crashed?

    return ((@state=="done" && @exit_status!=0) || error_state?)

  end

  #####################################################
  #
  # expired?
  #
  #####################################################
  def expired?

    # Check to see if the start timeout has expired
    if !@start_timeout.nil? && Time.now > @start_timeout      
      return (!running? && !done_okay? && @state!="unknown")
    end

    # Check to see if the end timeout has expired
    if !@end_timeout.nil? && Time.now > @end_timeout
      return (!done_okay?  && @state!="unknown")
    end

    return false

  end


  #####################################################
  #
  # print_crash_report
  #
  #####################################################
  def print_crash_report

    begin
      puts
      info=@scheduler.info
      if info.nil?
        puts "Batch system info unavailable"
      else
        puts info
      end

      puts

      qstat=@scheduler.qstat
      if qstat.nil?
        puts "Batch system queue status unavailable"
      else
        puts qstat
      end

      puts

      puts "Job output from: #{@attributes['-o']}"
      puts
      IO.foreach(@attributes['-o']) { |line|
        puts line
      }

    rescue
      puts $!
    end

  end

end

$__job__ == __FILE__
end
