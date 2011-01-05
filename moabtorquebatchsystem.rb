unless defined? $__moabtorquebatchsystem__

##########################################
#
# Class MoabTorqueBatchSystem
#
##########################################
class MoabTorqueBatchSystem

  require 'etc'
  require 'command.rb'
  require 'exceptions.rb'
  require 'parsedate'
  require 'libxml.rb'

  @@qstat_refresh_rate=30
  @@max_history=3600*1

  #####################################################
  #
  # initialize
  #
  #####################################################
  def initialize(moab_root=nil,torque_root=nil,qstat_refresh_rate=@@qstat_refresh_rate)

    begin

      # Set the path to the Moab commands
      if moab_root.nil?
        if ENV['MOAB_ROOT'].nil?
          @moab_root="/opt/moab/default/bin"
        else
          @moab_root=ENV['MOAB_ROOT']
        end
      else
        @moab_root=moab_root
      end
      ENV['MOAB_ROOT']=@moab_root

      # Set the path to the Torque commands
      if torque_root.nil?
        if ENV['TORQUE_ROOT'].nil?
          @torque_root="/opt/torque/default/bin"
        else
          @torque_root=ENV['TORQUE_ROOT']
        end
      else
        @torque_root=torque_root
      end
      ENV['TORQUE_ROOT']=@torque_root

      # Initialize hashes to store qstat output and exit records
      @qstat=Hash.new
      @exit_records=Hash.new

      # Set the qstat refresh rate and availability flag
      @qstat_refresh_rate=qstat_refresh_rate
      @qstat_available=true

      # Initialize the qstat table with current data
#      self.refresh_qstat

    rescue
      raise "MoabTorqueBatchSystem object could not be initialized\n\n#{$!}"
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
      output=Command.run("#{@torque_root}/qstat -u #{username}")
      if output[1] != 0
        raise output[0]
      else
        @qstat_update_time=Time.now
        output[0].each { |s|
          jobdata=s.strip.split(/\s+/)
          next unless jobdata[0]=~/^\d+/

          # Put state into a form wfm understands
          if jobdata[9] == "C"
	    jobdata[9]="done"
          end
          @qstat[jobdata[0].gsub(".nc","")]=jobdata[9]
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

      # Get the username of this process
      username=Etc.getpwuid(Process.uid).name

      # run showq to obtain the current status of queued jobs
#      output=Command.run("#{@moab_root}/showq -c -u #{username} 2>&1")
      output=Command.run("#{@moab_root}/showq -c --xml -u #{username} 2>&1")
      if output[1] != 0
        raise output[0]
      else
        recordxmldoc=LibXML::XML::Parser.string(output[0]).parse
        recordxml=recordxmldoc.root
        recordxml.find('//job').each { |job|
          fields=Hash.new
          fields['exit_status']=job.attributes['CompletionCode'].to_i
          fields['submit_time']=Time.at(job.attributes['SubmissionTime'].to_i)
          fields['start_time']=Time.at(job.attributes['StartTime'].to_i)
          fields['end_time']=Time.at(job.attributes['CompletionTime'].to_i)
          @exit_records[job.attributes['JobID']]=fields
        }
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
    if @qstat_update_time.nil?
      self.refresh_qstat
    else
      self.refresh_qstat if (Time.now - @qstat_update_time) > @qstat_refresh_rate
    end

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
  def get_job_exit_record(torque_jid,max_age=86400)

    # Get moab jid from torque jid
    jid=torque_jid.split(".")[0]

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

    return @exit_records[jid]

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
      cmd="qsub"
      attributes.each { |attr,value|
        cmd=cmd+" #{attr} #{value}"
      }
      cmd=cmd+" #{script}"

      # Issue the submit command
      output=Command.run("#{@torque_root}/#{cmd} 2>&1")
      if output[1] != 0
        raise "#{output[0]}"
      end

      # Check for success
      if (output[0]=~/(\d+\.\w+)/)
        return $1
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
      output=Command.run("#{@torque_root}/qdel #{jid}")
      if output[1] != 0
        raise output[0]
      end
      return 0

    rescue
      puts "ERROR: qdel #{jid} failed"
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
      output=Command.run(". #{@moabtorque_env}/profile.moabtorque; bjobs")
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

$__moabtorquebatchsystem__ == __FILE__

end
