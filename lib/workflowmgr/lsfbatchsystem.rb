##########################################
#
# Module WorkflowMgr
#
##########################################
module WorkflowMgr

  ##########################################
  #
  # Class LSFBatchSystem 
  #
  ##########################################
  class LSFBatchSystem

    require 'fileutils'
    require 'etc'

    @@qstat_refresh_rate=30
    @@max_history=3600*1

    #####################################################
    #
    # initialize
    #
    #####################################################
    def initialize


      # Initialize an empty hash for job queue records
      @jobqueue={}

      # Initialize an empty hash for job accounting records
      @jobacct={}

      # Initialize the hrs back contained in the jobacct hash
      @hrsback=0

    end


    #####################################################
    #
    # status
    #
    #####################################################
    def status(jobid)

      # Populate the jobs status table if it is empty
      refresh_jobqueue if @jobqueue.empty?

      # Return the jobqueue record if there is one
      return @jobqueue[jobid] if @jobqueue.has_key?(jobid)

      # If we didn't find the job in the jobqueue, look for it in the accounting records

      # Populate the job accounting log table if it is empty
      refresh_jobacct if @jobacct.empty?

      # Return the jobacct record if there is one
      return @jobacct[jobid] if @jobacct.has_key?(jobid)

      # If we still didn't find the job, look 72 hours back if we haven't already
      if @hrsback < 72
	refresh_jobacct(72)
	return @jobacct[jobid] if @jobacct.has_key?(jobid)
      end

      # We didn't find the job, so return an uknown status record
      return { :jobid => jobid, :state => "UNKNOWN", :native_state => "Unknown" }

    end

    #####################################################
    #
    # submit
    #
    #####################################################
    def submit(task)

puts "lsfbatchsystem: submit"

      # Initialize the submit command
      cmd="bsub"

      # Add LSF batch system options translated from the generic options specification
      task.attributes.each do |option,value|
        case option
          when :account
            cmd += " -P #{value}"
          when :queue            
            cmd += " -q #{value}"
          when :cores
            cmd += " -n #{value}"
          when :walltime
            cmd += " -W #{value}"
          when :memory
            cmd += " -M #{value}"
          when :stdout
	    FileUtils.mkdir_p(File.dirname(value))
            cmd += " -o #{value}"
          when :stderr
	    FileUtils.mkdir_p(File.dirname(value))
            cmd += " -e #{value}"
          when :join
	    FileUtils.mkdir_p(File.dirname(value))
            cmd += " -o #{value}"           
          when :jobname
            cmd += " -J #{value}"
          when :native
	    cmd += " #{value}"
        end
      end

      # LSF does not have an option to pass environment vars
      # Instead, the vars must be set in the environment before submission
      task.envars.each { |name,env|
        if env.nil?
          ENV['#{name}']=""
        else
          ENV['#{name}']="#{env}"
        end
      }

      # Add the command to submit
      cmd += " #{task.attributes[:command]}"

      # Run the submit command
      output=`#{cmd} 2>&1`.chomp
puts "LSFBatchSystem: output=#{output}"
      # Parse the output of the submit command
      if output=~/Job <(\d+)> is submitted to queue/
        return $1,output
      else
 	return nil,output
      end

    end


    #####################################################
    #
    # delete
    #
    #####################################################
    def delete(jobid)

      qdel=`bkill #{jobid}`      

    end

private

    #####################################################
    #
    # refresh_jobqueue
    #
    #####################################################
    def refresh_jobqueue

      # run bjobs to obtain the current status of queued jobs
      queued_jobs=`bjobs -w`

      # Parse the output of bjobs, building job status records for each job
      queued_jobs.each { |s|
        jobdata=s.strip.split(/\s+/)
        next unless jobdata[0]=~/^\d+$/
        next unless jobdata[1]=~/^#{username}$/
        @qstat[jobdata[0].to_i]=jobdata[2]
      }

#      if output[1] != 0
#        raise output[0]
#      else
#        @qstat_update_time=Time.now
#        output[0].each { |s|
#          jobdata=s.strip.split(/\s+/)
#          next unless jobdata[0]=~/^\d+$/
#          next unless jobdata[1]=~/^#{username}$/
#          @qstat[jobdata[0].to_i]=jobdata[2]
#        }        
#      end

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

        # LSF on Bluefire seems to have ~6 logfiles per 24 hours.
        # 3 files for max_history (1hr) should be more than enough.
        # If not parse 10 log files.
        if max_history == @@max_history
         n_logfiles = 3
        else
         n_logfiles = 10
        end

        # run bhist to obtain the current status of queued jobs
        output=Command.run(". #{@lsf_env}/profile.lsf; bhist -n #{n_logfiles} -l -d -w -C #{min_end_time.strftime("%Y/%m/%d/%H:%M")}, 2>&1")

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
  	  # Create the path for the log file
	  if (attr == "-o" or attr == "-e") then
	    FileUtils.mkdir_p(File.dirname(value))
          end
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

end
