unless defined? $__loadlevelerbatchsystem__

require 'workflowmgr/batchsystem'

##########################################
#
# Class LoadLevelerBatchSystem
#
##########################################
class LoadLevelerBatchSystem < BatchSystem

  require 'etc'
  require 'parsedate'
  require 'date'
  require 'command.rb'
  require 'exceptions.rb'

  @@qstat_refresh_rate=30
  @@max_history=3600*1

  @@translation_table={
                      "C"  => "C",
                      "CA" => "CA",
                      "CK" => "CK",
                      "CP" => "CP",
                      "D"  => "D",
                      "E"  => "E",
                      "EP" => "EP",
                      "H"  => "H",
                      "HS" => "HS",
                      "I"  => "I",
                      "MP" => "MP",
                      "NR" => "NR",
                      "NQ" => "NQ",
                      "P"  => "P",
                      "R"  => "Running",
                      "RM" => "RM",
                      "RP" => "RP",
                      "S"  => "S",
                      "ST" => "ST",
                      "SX" => "SX",
                      "TX" => "TX",
                      "V"  => "V",
                      "VP" => "VP",
                      "X"  => "X",
                      "XP" => "XP"                      
                      }

  ########################################################
  #                  
  # LoadLeveler jobs can be in any of the following states
  #
  # C   Completed
  # CA  Canceled
  # CK  Checkpointing
  # CP  Complete Pending
  # D   Deferred
  # E   Preempted
  # EP  Preempt Pending
  # H   User Hold
  # HS  User Hold and System Hold
  # I   Idle
  # MP  Resume Pending
  # NR  Not Run
  # NQ  Not Queued
  # P   Pending
  # R   Running
  # RM  Removed
  # RP  Remove Pending
  # S   System Hold
  # ST  Starting
  # SX  Submission Error
  # TX  Terminated
  # V   Vacated
  # VP  Vacate Pending
  # X   Rejected
  # XP  Reject Pending
  #
  ########################################################


  #####################################################
  #
  # initialize
  #
  #####################################################
#  def initialize(ll_root="/ssg/loadl",qstat_refresh_rate=@@qstat_refresh_rate)
  def initialize(ll_root="/usr/lpp/LoadL/full",qstat_refresh_rate=@@qstat_refresh_rate)

    begin

      # Set the root of LoadLeveler install
      @ll_root=ll_root

      # Set path to LoadLeveler commands
      @ll_path="#{@ll_root}/bin"
      @acct_path="/home/bluesky/loadl/spool"

      # Initialize hashes to store qstat output and exit records
      @qstat=Hash.new
      @exit_records=Hash.new

      # Set the qstat refresh rate and availability flag
      @qstat_refresh_rate=qstat_refresh_rate

      @qstat_available=true

      # Initialize the qstat table with current data
      self.refresh_qstat

    rescue
      raise "LoadLevelerBatchSystem object could not be initialized\n\n#{$!}"
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

      # run qstat to obtain the current status of queued jobs
      output=Command.run("#{@ll_path}/llq -u #{username} -f %id %st")
      if output[1] != 0
        raise output[0]
      else
        @qstat_update_time=Time.now
        output[0].each { |s|
          jobdata=s.strip.split(/\s+/)
          next unless jobdata[0]=~/^(\w+\.\d+)\.0$/
          @qstat[$1]=jobdata[1]
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
  # get_job_generic_state
  #
  #####################################################
  def get_job_generic_state(jid)

    return @@translation_table[self.get_job_state(jid)]

  end


  #####################################################
  #
  # get_job_exit_record
  #
  #####################################################
  def get_job_exit_record(jid,max_age=86400)

    # Get the username of this process
    username=Etc.getpwuid(Process.uid).name

    # Return nil if the exit record file doesn't exist
    return nil unless File.exists?("/ptmp/#{username}/#{jid}")

    # Get the fields out of the exit record
    lines=File.readlines("/ptmp/#{username}/#{jid}")

    # Parse the exit record
    exit_record=Hash.new
    lines.each { |line|
      case line
        when /^\s*Job Step Id: (\w+\.\d+)/
          exit_record['jid']=$1          
        when /^\s*Allocated Host: (\w+)/
          exit_record['exec_host']=$1
        when /^\s*Queue Date: (.+)$/
          date_arr=ParseDate::parsedate($1,false)          
          exit_record['submit_time']=Time.local(*date_arr)
        when /^\s*Dispatch Time: (.+)$/
          date_arr=ParseDate::parsedate($1,false)
          exit_record['start_time']=Time.local(*date_arr)
        when /^\s*Completion Date: (.+)$/
          date_arr=ParseDate::parsedate($1,false)
          exit_record['end_time']=Time.local(*date_arr)
       when /^\s*Completion Code: (\d+)$/
          exit_record['exit_status']=$1.to_i >> 8
      end
    }
    exit_record['start_time'] = exit_record['submit_time'] unless exit_record.has_key?('start_time')

    return exit_record

  end


  #####################################################
  #
  # get_job_exit_status
  #
  #####################################################
  def get_job_exit_status(jid,max_age=86400)

puts "get_job_exit_status: #{jid}"
    record=get_job_exit_record(jid,max_age)
puts "get_job_exit_status: #{record.inspect}"
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

      # Get the username of this process
      username=Etc.getpwuid(Process.uid).name

      # Open a pipe to the llsubmit command
      IO.popen("#{@ll_path}/llsubmit - 2>&1","w+") { |io|

        # Send keywords to run script as first step
        puts("\#\@ step_name = script")
        io.puts("\#\@ step_name = script")

        # Send keyword to set executable name
        puts("\#\@ executable = #{script}")
        io.puts("\#\@ executable = #{script}")

        # Send keywords corresponding to other job properties        
        attributes.each { |attr,value|
          if value.nil?
            puts("\#\@ #{attr}")
            io.puts("\#\@ #{attr}")
          else
            puts("\#\@ #{attr} = #{value}")
            io.puts("\#\@ #{attr} = #{value}")
          end
        }

        # Send keywords to queue the first step
        puts("\#\@ queue")
        io.puts("\#\@ queue")

        # Now write commands to create a step to retrieve the exit status
        puts("\#\@ step_name = script_exit")
        io.puts("\#\@ step_name = script_exit")
#CWH        puts("\#\@ executable = /ssg/loadl/bin/llq")        
#CWH        io.puts("\#\@ executable = /ssg/loadl/bin/llq")
        puts("\#\@ executable = /usr/lpp/LoadL/full/bin/llq")
        io.puts("\#\@ executable = /usr/lpp/LoadL/full/bin/llq")
        puts("\#\@ arguments = -l $(schedd_host).$(jobid).0 > /ptmp/#{username}/$(schedd_host).$(jobid)")
        io.puts("\#\@ arguments = -l $(schedd_host).$(jobid).0 > /ptmp/#{username}/$(schedd_host).$(jobid)")
        puts("\#\@ dependency = (script >= 0 || script <=0)")
        io.puts("\#\@ dependency = (script >= 0 || script <=0)")
        puts("\#\@ wall_clock_limit = 00:01:00")
        io.puts("\#\@ wall_clock_limit = 00:01:00")
        puts("\#\@ job_type = serial")
        io.puts("\#\@ job_type = serial")
        puts("\#\@ class = 1")
        io.puts("\#\@ class = 1")
        puts("\#\@ node_usage = shared")
        io.puts("\#\@ node_usage = shared")
        puts("\#\@ resources = ConsumableMemory(100 MB)")
        io.puts("\#\@ resources = ConsumableMemory(100 MB)")
        puts("\#\@ task_affinity = cpu(1)")
        io.puts("\#\@ task_affinity = cpu(1)")
        puts("\#\@ output = /dev/null")
        io.puts("\#\@ output = /dev/null")
        puts("\#\@ error = /dev/null")
        io.puts("\#\@ error = /dev/null")

        # Send keywords to queue the second step
        puts("\#\@ queue")
        io.puts("\#\@ queue")

        # Close and flush the write end of the pipe to send output to llsubmit
        io.close_write

        # Read the output of llsubmit
        output=io.readlines

        # Check for success
        output.each { |line|
          if (line=~/llsubmit: The job "(\w+\.\d+)" with 2 job steps has been submitted./)
            return $1
          end
        }

        # If we are here, it means the submit failed
        raise output.join

      }

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

      # Run qdel to delete the job
      output=Command.run("#{@ll_path}/llcancel #{jid}")
      if output[1] != 0
        raise output[0]
      end
      return 0

    rescue
      puts "ERROR: #{@ll_path}/llcancel #{jid} failed"
      puts $!
      return 1
    end

  end


  #####################################################
  #
  # info
  #
  #####################################################
  def info

    begin

      output=Command.run("#{@ll_path}/llstatus")
      if output[1] != 0
        raise output[0]
      end
      return output[0]

    rescue
      puts "ERROR: #{@ll_path}/llstatus failed"
      puts $!
      return nil
    end

  end


  #####################################################
  #
  # qstat
  #
  #####################################################
  def qstat

    begin

      # Run llq
      output=Command.run("#{@ll_path}/llq")
      if output[1] != 0
        raise output[0]
      end
      return output[0]

    rescue
      puts "ERROR: #{@ll_path}/llq failed"
      puts $!
      return nil
    end

  end

end

$__loadlevelerbatchsystem__ == __FILE__

end
