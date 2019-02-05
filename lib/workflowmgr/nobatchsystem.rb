unless defined? $__nobatchsystem__

##########################################
#
# Class NoBatchSystem
#
##########################################
class NoBatchSystem

  require 'command.rb'
  require 'exceptions.rb'

  @@qstat_refresh_rate=30

  require 'workflowmgr/batchsystem'

  #####################################################
  #
  # initialize
  #
  #####################################################
  def initialize(qstat_refresh_rate=@@qstat_refresh_rate) < BatchSystem

    begin

      # Initialize hashes to store qstat output and exit records
      @qstat=Hash.new
      @exit_records=Hash.new

      # Set the qstat refresh rate and availability flag
      @qstat_refresh_rate=qstat_refresh_rate
      @qstat_available=true

      # Initialize the qstat table with current data
      self.refresh_qstat

    rescue
      raise "NoBatchSystem object could not be initialized\n\n#{$!}"
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
      output=Command.run("ps -u #{username} -f %id %st")
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

    begin

      # run qstat to obtain the job's state
      output=Command.run("ps | grep #{jid}")
      if output[1] != 0
        raise output[0]
      else
        state="done"
        output[0].each { |s|
          if s=~/^\s*#{jid}\s+.*$/
            state="r"
            break
          end
        }
      end
      return state

    rescue
      puts "ERROR: The state of job '#{jid}' could not be determined"
      puts $!
      return "unknown"
    end

  end




  #####################################################
  #
  # get_job_exit_status
  #
  #####################################################
  def get_job_exit_status(jid,max_age=86400)

    begin

      # Set the No_ROOT
      ENV['No_ROOT']=@sge_root

      # Get the accounting record for the job
      record=find_job(jid,max_age)
      if record.nil?
        exit_status=nil
      else
        exit_status=record.split(":")[12].to_i
        if exit_status==0
          exit_status=record.split(":")[11].to_i
        end
      end
      return exit_status

    rescue
      puts "Exit status for job #{jid} could not be found\n\n#{$!}"
      return nil
    end

  end


  #####################################################
  #
  # find_job
  #
  #####################################################
  def find_job(jid,max_age=86400)

    begin

      # Set the SGE_ROOT
      ENV['SGE_ROOT']=@sge_root

      # Get the current SGE server's hostname
      output=Command.run("#{@sge_path}/qconf -sss")
      if output[1] != 0
        raise output[0]
      end
      server=output[0].split(".")[0].chomp

      # Get our hostname
      host=`hostname -s`.chomp

      # If we are not on the server, invoke this method through an ssh tunnel to the server
      if server != host
        cmd="ssh -o StrictHostKeyChecking=no #{server} /usr/bin/ruby -r #{__FILE__} -e \\''puts SGEBatchSystem.new(\"#{@sge_root}\").find_job(#{jid},#{max_age})'\\'"
        output=Command.run(cmd)
        if output[1] != 0
          raise output[0]
        else
          record=output[0].chomp
          if record=="nil"
            return nil
          else
            return record
          end
        end
      end

      # Calculate the minimum end time we should look at
      min_end_time=Time.now - max_age

      # Look for the job record
      record=nil
      error=""
      catch (:done) do
        2.times do

          # Get a list of accounting files sorted by modification time in reverse order
          files=Dir["#{@acct_path}/accounting*"].sort! { |a,b|
            File.stat(b).mtime <=> File.stat(a).mtime
          }

          # Loop over files reading each one backwards
          count=0
          files.each { |file|
            if file=~/\.gz$/
              fd=IO.popen("gunzip -c #{file} | tac 2>&1")
            else
              fd=IO.popen("tac #{file} 2>&1")
            end
            fd.each { |line|
              error=line
              fields=line.split(/:/)

              # Quit if we've reached the minimum end_time
              end_time=Time.at(fields[10].to_i)
              if end_time > Time.at(0) && end_time < Time.at(min_end_time)
                count=count+1
              else
                count=0
              end
              if count > 10
                fd.close
                throw :done
              end

              # If the jid field matches, return the record
              if fields[5]=~/^#{jid}$/
                record=line
                fd.close
                throw :done
              end
            }
            fd.close unless fd.closed?
            if $? != 0
              if error=~/No such file or directory/
                files=Dir["#{@acct_path}/accounting*"].sort! { |a,b|
                  File.stat(b).mtime <=> File.stat(a).mtime
                }
                retry
              else
                raise error
              end
            end
          }

          sleep 1

        end     # 2.times
      end       # catch

      return record

    rescue
      puts "Accounting record for job #{jid} could not be found\n\n#{$!}"
      return nil
    end

  end


  #####################################################
  #
  # find_all_jobs
  #
  #####################################################
  def find_all_jobs(stime,etime,accounts,users,job_pattern,*attrs)

    # Get a list of accounting files sorted by modification time in reverse order
    files=Dir["#{@acct_path}/accounting*"].sort! { |a,b|
      File.stat(b).mtime <=> File.stat(a).mtime
    }

    # Get date strings for the times
    start_str=stime.strftime("%Y%m%d")
    end_str=etime.strftime("%Y%m%d")

    # Get the index of the first file to check
    sindex=files.length-1
    1.upto(files.length-1) { |index|
      if files[index].split(".")[1] < start_str
        sindex=index
        break
      end
    }

    # Get the index of the last file to check
    eindex=files.length-1
    1.upto(files.length-1) { |index|
      if files[index].split(".")[1] <= end_str
        eindex=index
        break
      end
    }
    if eindex==1
      eindex=0
    end

    jobs=Hash.new
    sindex.downto(eindex) { |index|
      file=files[index]
      if file=~/\.gz$/
        fd=IO.popen("gunzip -c #{file} | cat 2>&1")
      else
        fd=IO.popen("cat #{file} 2>&1")
      end

      while !fd.eof? do
        record=fd.gets
        if record=~/^#/
          next
        end
        fields=record.split(":")
        if stime.nil? || fields[10].to_i >= stime.to_i
          if etime.nil? || fields[10].to_i <= etime.to_i
            if accounts.nil? || !accounts.index(fields[6]).nil?
              if users.nil? || !users.index(fields[3]).nil?
                if job_pattern.nil? || job_pattern.match(fields[4])
                  job_key="#{fields[5]}_#{fields[8]}"
                  if attrs.empty?
                    jobs[job_key]=record
                  else
                    small_record=Array.new
                    attrs.each { |attr|
                      small_record.push(fields[attr.to_i])
                    }
                    jobs[job_key]=small_record
                  end
                end
              end
            end
          end
        end
      end
      fd.close
    }

    return jobs

  end


  #####################################################
  #
  # wait_job_start
  #
  #####################################################
  def wait_job_start(jid,timeout,interval,verbose)

    # Set the SGE_ROOT
    ENV['SGE_ROOT']=@sge_root

    # Calculate the expiration time
    expire_time=Time.now + timeout

    # Poll the job's state until it starts or the timeout expires
    state=get_job_state(jid)
    if verbose
      puts "#{Time.now} :: Job #{jid} is in state '#{state}'"
    end
    while (state != "r" && state != "done" && Time.now < expire_time)
      if expire_time - Time.now > interval
        sleep interval
      else
        sleep(expire_time - Time.now)
      end
      state=get_job_state(jid)
      if verbose
        puts "#{Time.now} :: Job #{jid} is in state '#{state}'"
      end
    end

    # Raise an exception if the timeout expired
    if (state != "r" && state != "done")
      if verbose
        puts "#{Time.now} :: Timeout expired!"
      end
      raise TimeoutExpired,"Job #{jid} did not start in time"
    else
      return 0
    end

  end


  #####################################################
  #
  # wait_job_finish
  #
  #####################################################
  def wait_job_finish(jid,timeout,interval,verbose)

    # Set the SGE_ROOT
    ENV['SGE_ROOT']=@sge_root

    # Calculate the expiration time
    expire_time=Time.now + timeout

    # Poll the job's state until it is done or the timeout has expired
    state=get_job_state(jid)
    if verbose
      puts "#{Time.now} :: Job #{jid} is in state '#{state}'"
    end
    while (state != "done" && Time.now < expire_time)
      if expire_time - Time.now > interval
        sleep interval
      else
        sleep(expire_time - Time.now)
      end
      state=get_job_state(jid)
      if verbose
        puts "#{Time.now} :: Job #{jid} is in state '#{state}'"
      end
    end

    # If the timeout has expired, raise an exception
    if (state != "done")
      if verbose
        puts "#{Time.now} :: Timeout expired!"
      end
      raise TimeoutExpired,"Job #{jid} did not finish in time"
    else
      return 0
    end

  end


  #####################################################
  #
  # submit
  #
  #####################################################
  def submit(script,attributes)

    begin

      # Issue the submit command
      output=Command.run("& #{script} 2>&1 ")
      if output[1] != 0
        raise "#{output[0]}"
      end

      # Check for success
      if (output[0]=~/[Yy]our job (\d+) .* has been submitted/)
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

      # Set the SGE_ROOT
      ENV['SGE_ROOT']=@sge_root

      # Run qdel to delete the job
      output=Command.run("#{@sge_path}/qdel #{jid}")
      if output[1] != 0
        raise output[0]
      end
      return 0

    rescue
      puts "ERROR: #{@sge_path}/qdel #{jid} failed"
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

      # Set the SGE_ROOT
      ENV['SGE_ROOT']=@sge_root

      # Run qdel to delete the job
      output=Command.run("/usr/local/fsl/bin/sgeinfo")
      if output[1] != 0
        raise output[0]
      end
      return output[0]

    rescue
      puts "ERROR: /usr/local/fsl/bin/sgeinfo failed"
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

      # Set the SGE_ROOT
      ENV['SGE_ROOT']=@sge_root

      # Run qdel to delete the job
      output=Command.run("/usr/local/fsl/bin/sgestat")
      if output[1] != 0
        raise output[0]
      end
      return output[0]

    rescue
      puts "ERROR: /usr/local/fsl/bin/sgestat failed"
      puts $!
      return nil
    end

  end


  #####################################################
  #
  # rollover
  #
  #####################################################
  def rollover

    begin

      # Set the SGE_ROOT
      ENV['SGE_ROOT']=@sge_root

      # Get the current SGE server's hostname
      output=Command.run("#{@sge_path}/qconf -sss")
      if output[1] != 0
        raise output[0]
      end
      server=output[0].split(".")[0].chomp

      # Get our hostname
      host=`hostname -s`.chomp

      # Don't rollover unless we are on the current server
      return 1 unless host==server

      # Get a list of accounting files sorted by modification time in reverse order
      files=Dir["#{@acct_path}/accounting*"].sort! { |a,b|
        File.stat(b).mtime <=> File.stat(a).mtime
      }

      # Get the date of the first record in the accounting file
      end_time=-1
      file=File.new(files[0])
      file.each { |line|
        next if line=~/^#/
        end_time=Time.at(line.split(":")[10].to_i)
        break
      }
      return 0 if end_time==-1

      # Calculate the name of the new accounting file
      date_str=end_time.strftime("%Y%m%d")
      fname="#{@acct_path}/accounting.#{date_str}"

      # If the file already exists roll it over
      if File.exists?(fname)

        # Find all files associated with that date sorted by modification date
        oldfiles=files.find_all { |file|
          file=~/^#{@acct_path}\/accounting\.#{date_str}/
        }.sort! { |a,b|
          File.stat(a).mtime <=> File.stat(b).mtime
        }

        # Roll them over in reverse order
        oldfiles.each { |file|
          # Get the new extension for the file
          if file=~/^#{@acct_path}\/accounting\.\d+\.(\d+)$/
            ext=$1.to_i + 1
          else
            ext=0
          end

          # Move the file to its new name
          `mv #{file} #{@acct_path}/accounting.#{date_str}.#{ext}`
        }

      end

      # Move the file to it's new name
      `mv #{@acct_path}/accounting #{fname}`

      # Touch a new accounting file
      `touch #{@acct_path}/accounting`

      # Gzip files older than 1 week
      files.reject { |file|
        file=~/\.gz$/ || (Time.now - File.stat(file).mtime < 60*60*24*7)
      }.each { |file|
        `/bin/gzip #{file}`
      }

      # Return success
      return 0

    rescue
      puts "ERROR: Rollover failed"
      puts $!
      return 1
    end
  end

end

$__nobatchsystem__ == __FILE__
end
