unless defined? $__lsfbatchsystem__

##########################################
#
# Class LSFBatchSystem
#
##########################################
class LSFBatchSystem

  require 'etc'
  require 'command.rb'
  require 'exceptions.rb'

  @@qstat_refresh_rate=30
  @@max_history=3600*1

  #####################################################
  #
  # initialize
  #
  #####################################################
  def initialize(lsf_env=nil,qstat_refresh_rate=@@qstat_refresh_rate)

    begin

      # Set the path to the LSF commands
      if lsf_env.nil?
        if ENV['LSF_ENVDIR'].nil?
          @lsf_env="/usr/local/lsf/conf" 
        else
          @lsf_env=ENV['LSF_ENVDIR']
        end
      else
        @lsf_env=lsf_env
      end
      ENV['LSF_ENVDIR']=@lsf_env

      # Initialize hashes to store qstat output and exit records
      @qstat=Hash.new
      @exit_records=Hash.new

      # Set the qstat refresh rate and availability flag
      @qstat_refresh_rate=qstat_refresh_rate
      @qstat_available=true

      # Initialize the qstat table with current data
      self.refresh_qstat

    rescue
      raise "LSFBatchSystem object could not be initialized\n\n#{$!}"
    end
    
  end


  #####################################################
  #
  # refresh_qstat
  #
  #####################################################
  def refresh_qstat

    begin

      # Clear the previous qstat data
      @qstat.clear

      # Reset qstat availability flag
      @qstat_available=true

      # Get the username of this process
      username=Etc.getpwuid(Process.uid).name

      # run bjobs to obtain the current status of queued jobs
      output=Command.run(". #{@lsf_env}/profile.lsf; bjobs")
      if output[1] != 0
        raise output[0]
      else
        @qstat_update_time=Time.now
        output[0].each { |s|
          jobdata=s.strip.split(/\s+/)
          next unless jobdata[0]=~/^\d+$/
          next unless jobdata[1]=~/^#{username}$/
          @qstat[jobdata[0].to_i]=jobdata[2]
        }        
      end

    rescue 
      @qstat_available=false
      puts $!
      return
    end

  end


  #####################################################
  #
  # refresh_exit_record
  #
  #####################################################
  def refresh_exit_record(max_history=@@max_history)

    begin

      # Clear the previous exit_record data
      @exit_records.clear

      # Calculate the minimum end time we should look at
      min_end_time=Time.now-max_history

      # run bhist to obtain the current status of queued jobs
      output=Command.run(". #{@lsf_env}/profile.lsf; bhist -n 3 -l -d -w -C #{min_end_time.strftime("%Y/%m/%d/%H:%M")}, 2>&1")
      if output[1] != 0
        raise output[0]
      else
        @qstat_update_time=Time.now
        jid=nil
        record=""
        output[0].each { |s|
          if s=~/^Job <(\d+)>/
            @exit_records[jid]=record unless jid.nil?
            jid=$1.to_i
            record="#{s}\n"
          else
            record=record+"#{s}\n" unless jid.nil?
          end
        }        
        @exit_records[jid]=record unless jid.nil?
      end

    rescue
      puts $!
      return
    end   

  end


  #####################################################
  #
  # get_job_state
  #
  #####################################################
  def get_job_state(jid)

    # Refresh qstat table if we need to
    self.refresh_qstat if (Time.now - @qstat_update_time) > @qstat_refresh_rate

    # Check qstat table for job state
    if @qstat.has_key?(jid)
      state=@qstat[jid]
    else
      if @qstat_available
        state="done"
      else
        state="unknown"
      end
    end
      
    return state

  end


  #####################################################
  #
  # get_job_exit_record
  #
  #####################################################
  def get_job_exit_record(jid,max_age=86400)

    require 'parsedate'

    # If the exit record is not in the table, refresh the table
    unless @exit_records.has_key?(jid)

      # Refresh with default history length
      self.refresh_exit_record

      # If the exit record is still not in the table, refresh the table with max_age history length
      unless @exit_records.has_key?(jid)

        # Wait a second in case LSF server is slow in writing the record to the accounting file
        sleep 1

        # Refresh with max_age history length
        self.refresh_exit_record(max_age)

        # If the exit record is STILL not in the table, assume it will never be found and give up
        return nil unless @exit_records.has_key?(jid)

      end

    end

    # Get the raw exit record string for jid
    recordstring=@exit_records[jid]

    # Try to format the record such that it is easier to parse
    recordstring.gsub!(/\n\s{3,}/,'')
    recordstring.gsub!(/, Command <.*>/,'')

    # Build the exit record
    exit_record=Hash.new
    now=Time.now

    # Initialize the exit status to 137 (assume it was killed if we can't determine the exit status)
    exit_record['exit_status']=137

    # Parse the jid
    exit_record['jid']=recordstring.match(/Job <(\d+)>/)[1].to_i

    # Parse the execution host
    match=@exit_records[jid].match(/^.*: Dispatched to (\d+ Hosts\/Processors ){0,1}(<.+>)+/)
    unless match.nil?
      exit_record['exec_host']=match.captures[1].gsub(/<|>/,"").gsub(/\d+\*/,"").split(/\s+/)
    end

    # Parse the submit time
    match=@exit_records[jid].match(/(.*): Submitted from host/)
    unless match.nil?
      temptime=ParseDate.parsedate(match.captures.first,true)
      if temptime[0].nil?
        temptime[0]=now.year
        if Time.local(*temptime) > now 
          temptime[0]=now.year-1
        end
      end
      exit_record['submit_time']=Time.local(*temptime)
    end

    # Parse the start time
    match=@exit_records[jid].match(/^(.*): Running with execution home/)
    unless match.nil?
      temptime=ParseDate.parsedate(match.captures.first,true)
      if temptime[0].nil?
        temptime[0]=now.year
        if Time.local(*temptime) > now 
          temptime[0]=now.year-1
        end
      end
      exit_record['start_time']=Time.local(*temptime)
    end

    # Parse the end time if the job succeeded
    match=@exit_records[jid].match(/^(.*): Done successfully/)
    unless match.nil?
      temptime=ParseDate.parsedate(match.captures.first,true)
      if temptime[0].nil?
        temptime[0]=now.year
        if Time.local(*temptime) > now 
          temptime[0]=now.year-1
        end
      end
      exit_record['end_time']=Time.local(*temptime)
      exit_record['exit_status']=0
    end


    # Parse the end time if the job failed
    match=@exit_records[jid].match(/^(.*): Exited with exit code (\d+)/)
    unless match.nil?
      temptime=ParseDate.parsedate(match.captures.first,true)
      if temptime[0].nil?
        temptime[0]=now.year
        if Time.local(*temptime) > now 
          temptime[0]=now.year-1
        end
      end
      exit_record['end_time']=Time.local(*temptime)
      exit_record['exit_status']=match.captures[1].to_i
    end

    return exit_record
  
  end


  #####################################################
  #
  # get_job_exit_status
  #
  #####################################################
  def get_job_exit_status(jid,max_age=86400)

    record=get_job_exit_record(jid,max_age)
    if record.nil?
      puts "\nExit status for job #{jid} could not be found\n"
      return nil
    else
      return record['exit_status']
    end

  
  end


  #####################################################
  #
  # submit
  #
  #####################################################
  def submit(script,attributes)

    begin

      # Build the submit command
      cmd="bsub"
      attributes.each { |attr,value|
        cmd=cmd+" #{attr} #{value}"
      }
      cmd=cmd+" #{script}"

      # Issue the submit command
      output=Command.run(". #{@lsf_env}/profile.lsf; #{cmd} 2>&1")
      if output[1] != 0
        raise "#{output[0]}"
      end

      # Check for success
      if (output[0]=~/Job <(\d+)> is submitted to queue/)
        return $1.to_i
      else
        raise "#{output[0]}"
      end

    rescue
      raise $!
    end

  end

  #####################################################
  #
  # qdel
  #
  #####################################################
  def qdel(jid)

    begin

      # Run bkill to delete the job
      output=Command.run(". #{@lsf_env}/profile.lsf; bkill #{jid}")
      if output[1] != 0
        raise output[0]
      end
      return 0

    rescue
      puts "ERROR: bkill #{jid} failed"
      puts $!
      return 1
    end

  end


  #####################################################
  #
  # qstat
  #
  #####################################################
  def qstat

    begin

      # Run bjobs to get job status info
      output=Command.run(". #{@lsf_env}/profile.lsf; bjobs")
      if output[1] != 0
        raise output[0]
      end
      return output[0]

    rescue
      puts "ERROR: bjobs failed"
      puts $!
      return nil
    end

  end


end

$__lsfbatchsystem__ == __FILE__

end
