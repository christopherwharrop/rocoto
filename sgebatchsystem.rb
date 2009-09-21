unless defined? $__sgebatchsystem__

if File.symlink?(__FILE__)
  $:.unshift(File.dirname(File.readlink(__FILE__))) unless $:.include?(File.dirname(File.readlink(__FILE__))) 
else
  $:.unshift(File.dirname(__FILE__)) unless $:.include?(File.dirname(__FILE__)) 
end
$:.unshift("#{File.dirname(__FILE__)}/libxml-ruby-0.8.3/ext/libxml")

###############################################################################
#
# SGE accounting record format (fields are separated by colons)
#
#  0     qname		Name of the queue in which the job has run.
# 
#  1	hostname	Name of the execution host.
# 
#  2    group  		The effective group id of the job owner when 
#			executing the job.
# 
#  3    owner  		Owner of the Grid Engine job.
# 
#  4    job_name	Job name.
# 
#  5    job_number      Job identifier - job number.
# 
#  6    account         An account string as specified by the qsub(1) or 
#			qalter(1) -A option.
# 
#  7    priority	Priority value assigned to the job corresponding to 
#			the priority parameter in the queue configuration
#			(see queue_conf(5)).
# 
#  8    submission_time	Submission time in seconds (since epoch format).
# 
#  9    start_time	Start time in seconds (since epoch format).
#
# 10    end_time	End time in seconds (since epoch format).
# 
# 11    failed 		Indicates  the  problem which occurred in case a job 
#			could not be started on the execution host (e.g.
#			because the owner of the job did not have a valid 
#			account on that machine). If Grid Engine  tries  to
#			start a job multiple times, this may lead to multiple 
#			entries in the accounting file corresponding to the 
#			same job ID.
# 12    exit_status	Exit status of the job script (or Grid Engine specific 
#			status in case of certain error conditions).
# 
# 13    ru_wallclock	Difference between end_time and start_time (see above).
#
#       The remainder of the accounting entries follows the contents of the 
#	standard UNIX rusage structure  as described in getrusage(2).  The 
#	following entries are provided:
# 
# 14           ru_utime
# 15           ru_stime
# 16           ru_maxrss
# 17           ru_ixrss
# 18           ru_ismrss
# 19           ru_idrss
# 20           ru_isrss
# 21           ru_minflt
# 22           ru_majflt
# 23           ru_nswap
# 24           ru_inblock
# 25           ru_oublock
# 26           ru_msgsnd
# 27           ru_msgrcv
# 28           ru_nsignals
# 29           ru_nvcsw
# 30           ru_nivcsw
# 
# 31    project		The  project  which  was  assigned  to  the job. 
#			Projects are only supported in case of a Grid Engine
#			Enterprise Edition system.
# 
# 32    department	The department which was assigned to the job. 
#			Departments are only supported in case of a Grid Engine
#			Enterprise Edition system.
# 
# 33    granted_pe	The parallel environment which was selected for that 
#			job.
#
# 34	slots  		The number of slots which were dispatched to the job 
#			by the scheduler.
# 35    task_number	Job array task index number.
# 
# 36    cpu		The cpu time usage in seconds.
# 
# 37    mem		The integral memory usage in Gbytes seconds.
# 
# 38    io		The amount of data transferred in input/output 
#			operations.
# 
# 39    category	A string specifying the job category.
# 
# 40    iow		The io wait time in seconds.
# 
# 41    pe_taskid	If  this  identifier  is set the task was part of a 
#			parallel job and was passed to Grid Engine Enterprise
#			Edition via the qrsh inherit interface.
# 
# 42    maxvmem		The maximum vmem size in bytes.
#
###############################################################################


##########################################
#
# Class SGEBatchSystem
#
##########################################
class SGEBatchSystem

  require 'etc'
  require 'timeout'
  require 'command.rb'
  require 'exceptions.rb'

  @@qstat_refresh_rate=30
  @@max_history=3600*1
  @@qsub_wrapper="/usr/local/fsl/bin/qsub"
  @@refresh_timeout=15

  #####################################################
  #
  # initialize
  #
  #####################################################
  def initialize(sge_root=nil,qstat_refresh_rate=@@qstat_refresh_rate)

    begin

      # Set the SGE_ROOT
      if sge_root.nil?
        if ENV['SGE_ROOT'].nil?
          if File.exists?("/usr/local/sge/util/arch")
            @sge_root="/usr/local/sge"
          elsif File.exists?("/opt/sge/default/util/arch")
            @sge_root="/opt/sge/default"
          elsif File.exists?("/opt/sge/util/arch")
            @sge_root="/opt/sge"
          end
        else
          @sge_root=ENV['SGE_ROOT']
        end
      else
        @sge_root=sge_root
      end
      ENV['SGE_ROOT']=@sge_root

      # Set the path to the SGE commands and accounting files
      output=Command.run("#{@sge_root}/util/arch")
      if output[1] != 0
        raise output[0]
      else
        bin=output[0].chomp
      end
      @sge_path="#{@sge_root}/bin/#{bin}"
#      @acct_path="#{@sge_root}/default/common"
      @acct_path="#{File.expand_path(ENV['SGE_ROOT'])}/default/common/accounting"
      catch (:done) do
        loop do
          if File.symlink?(@acct_path)
            @acct_path=File.expand_path(File.readlink(@acct_path),@acct_path)
          else
            throw :done
          end
        end
      end
      @acct_path=File.dirname(@acct_path)

      # Check to see if the accounting files are available locally or only on the SGE server
      if File.exist?("#{@acct_path}/accounting")
        @acct_local=true
      else
        @acct_local=false
      end

      # Initialize hashes to store qstat output and exit records
      @qstat=Hash.new
      @exit_records=Hash.new

      # Set the qstat refresh rate and availability flag
      @qstat_refresh_rate=qstat_refresh_rate
      @qstat_available=true

      # Initialize the qstat table with current data
#      self.refresh_qstat

    rescue
      raise "SGEBatchSystem object could not be initialized\n\n#{$!}"
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

      # Set the SGE_ROOT 
      ENV['SGE_ROOT']=@sge_root

      # Get the username of this process
      username=Etc.getpwuid(Process.uid).name

      # run qstat to obtain the current status of queued jobs
      output=Command.run("#{@sge_path}/qstat")
      if output[1] != 0
        raise output[0]
      else
        @qstat_update_time=Time.now
        output[0].each { |s|
          jobdata=s.strip.split(/\s+/)
          next unless jobdata[0]=~/^\d+$/
#          next unless jobdata[3]=~/^#{username}$/
          @qstat[jobdata[0].to_i]=jobdata[4]
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

      # Set the SGE_ROOT 
      ENV['SGE_ROOT']=@sge_root

      # Get the current SGE server's hostname
      output=Command.run("#{@sge_path}/qconf -sss")
      if output[1] != 0
        raise output[0]
      end
      server=output[0].split(".")[0].chomp

      # Get our hostname
      host=`/bin/hostname -s`.chomp

      # Get the list of accounting files sorted in reverse order by mtime
      if (!@acct_local && server != host)

        # If we are not on the server, invoke this method through an ssh tunnel to the server
        cmd="ssh #{server} /usr/bin/ruby -e \\''puts \" BEGIN \", Dir[\"#{@acct_path}/accounting*\"].sort! { |a,b| File.mtime(b) <=> File.mtime(a)}, \" END \"'\\'"
        output=Command.run(cmd)
        if output[1] != 0
          raise output[0]
        else
          lines=output[0].split(/\s+/)
          files=lines.slice(lines.index("BEGIN")+1..lines.rindex("END")-1)
        end
      else
        files=Dir["#{@acct_path}/accounting*"].sort! { |a,b| File.mtime(b) <=> File.mtime(a) }
      end

      # Calculate the minimum end time we should look at
      min_end_time=Time.now-max_history

      # Loop over files reading each one backwards
      timeout(@@refresh_timeout) do
        catch(:done) do
          count=0
          files.each { |file|

            # Build the command to retrieve the records
            if (!@acct_local && server!= host)
              if file=~/\.gz$/          
                cmd="ssh #{server} 'gunzip -c #{file} | tac'"
              else
                cmd="ssh #{server} 'tac #{file}'"
              end
            else
              if file=~/\.gz$/          
                cmd="gunzip -c #{file} | tac "
              else
                cmd="tac #{file}"
              end
            end

            # Open a pipe to the command
            IO.popen(cmd,"r") {|pipe|
              while !pipe.eof?
                record=pipe.gets
                fields=record.split(/:/)

                # Skip bogus records
                next unless fields.length==43 || fields.length==44 || fields.length==45
                next unless fields[8].to_i > 0

                # Quit if we've reached the minimum end_time
                end_time=Time.at(fields[10].to_i)
                if end_time > Time.at(0) && end_time < Time.at(min_end_time)
                  count=count+1
                else
                  count=0
                end
                if count > 10
                  throw :done
                end

                # Add the record if it hasn't already been added
                @exit_records[fields[5].to_i]=record unless @exit_records.has_key?(fields[5].to_i)
              end # while !eof
            } # popen

          }  # files.each

        end # catch :done

      end # timeout

    rescue TimeoutError
      puts "WARNING: Timeout while running '#{cmd}'"
      return
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
    if @qstat.has_key?(jid.to_i)
      state=@qstat[jid.to_i]
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

    # If the exit record is not in the table, refresh the table
    unless @exit_records.has_key?(jid.to_i)

      # Refresh with default history length
      self.refresh_exit_record

      # If the exit record is still not in the table, refresh the table with max_age history length
      unless @exit_records.has_key?(jid.to_i)

        # Wait a second in case SGE server is slow in writing the record to the accounting file
        sleep 1

        # Refresh with max_age history length
        self.refresh_exit_record(max_age)

        # If the exit record is STILL not in the table, assume it will never be found and give up
        return nil unless @exit_records.has_key?(jid.to_i)

      end

    end

    # Get the accounting record for the job
    fields=@exit_records[jid.to_i].split(":")
    exit_record=Hash.new
    exit_record['jid']=fields[5].to_i
    exit_record['exec_host']=fields[1]
    exit_record['submit_time']=Time.at(fields[8].to_i)
    exit_record['start_time']=Time.at(fields[9].to_i)
    exit_record['end_time']=Time.at(fields[10].to_i)
    exit_record['exit_status']=fields[12].to_i
    if exit_record['exit_status']==0
      exit_record['exit_status']=fields[11].to_i
    end

    return exit_record
  
  end


  #####################################################
  #
  # get_job_exit_status
  #
  #####################################################
  def get_job_exit_status(jid,max_age=86400)

    record=get_job_exit_record(jid.to_i,max_age)
    if record.nil?
      puts "\nExit status for job #{jid.to_i} could not be found\n"
      return nil
    else
      return record['exit_status']
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
      if (!@acct_local && server != host)
        if File.symlink?(__FILE__)
          requirefile=File.expand_path(File.readlink(__FILE__))
        else
          requirefile=File.expand_path(__FILE__)
        end
#        cmd="ssh #{server} /usr/bin/ruby -r #{File.expand_path(__FILE__)} -e \\''puts SGEBatchSystem.new(\"#{@sge_root}\").find_job(#{jid.to_i},#{max_age})'\\'"
        cmd="ssh #{server} /usr/bin/ruby -r #{requirefile} -e \\''puts SGEBatchSystem.new(\"#{@sge_root}\").find_job(#{jid.to_i},#{max_age})'\\'"
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
              if fields[5]=~/^#{jid.to_i}$/
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
      puts "Accounting record for job #{jid.to_i} could not be found\n\n#{$!}"
      return nil
    end   

  end

  #####################################################
  #
  # collect_job_stats
  #
  #####################################################
  def collect_job_stats(stime,etime,accounts,users)

    # Set the SGE_ROOT 
    ENV['SGE_ROOT']=@sge_root

    unless @acct_local

      # Get the current SGE server's hostname
      output=Command.run("#{@sge_path}/qconf -sss")
      if output[1] != 0
        raise output[0]
      end
      server=output[0].split(".")[0].chomp

      # Get our hostname
      host=`hostname -s`.chomp

      # If we are not on the server, invoke this method through an ssh tunnel to the server
      if (!@acct_local && server != host)
        if File.symlink?(__FILE__)
          requirefile=File.expand_path(File.readlink(__FILE__))
        else
          requirefile=File.expand_path(__FILE__)
        end
#        cmd="ssh #{server} /usr/bin/ruby -r #{File.expand_path(__FILE__)} -e \\''puts SGEBatchSystem.new(\"#{@sge_root}\").collect_job_stats(\"#{stime}\",\"#{etime}\",\"#{accounts}\",\"#{users}\")'\\'"
        cmd="ssh #{server} /usr/bin/ruby -r #{requirefile} -e \\''puts SGEBatchSystem.new(\"#{@sge_root}\").collect_job_stats(\"#{stime}\",\"#{etime}\",\"#{accounts}\",\"#{users}\")'\\'"
        output=Command.run(cmd,600)
        if output[1] != 0
          raise output[0]
        else
          # Attempt to get rid of the !@#$ banner messages from the ssh command output
          record=output[0].chomp.split(/\n/).last
          if record=="nil"
            return nil
          else
            return record
          end
        end
      end

    end

    # Get the start and end times in seconds since epoch
    ssecs=Time.gm(*(stime.gsub(/[-_:]/,":").split(":"))).to_i
    esecs=Time.gm(*(etime.gsub(/[-_:]/,":").split(":"))).to_i

    # Get the start and end times in yyyymmdd format
    sdate=Time.at(ssecs).strftime("%Y%m%d")
    edate=Time.at(esecs).strftime("%Y%m%d")

    # Get a list of all accounting file sorted by modification time in reverse order
    files=Dir["#{@acct_path}/accounting*"].sort! { |a,b|
      File.stat(b).mtime <=> File.stat(a).mtime
    }

    # Get the index of the first file to check
    sindex=files.length-1
    1.upto(files.length-1) { |index|
      if File.basename(files[index]).split(".")[1] < sdate
        sindex=index
        break
      end
    }

    # Get the index of the last file to check
    eindex=files.length-1
    1.upto(files.length-1) { |index|
      if File.basename(files[index]).split(".")[1] <= edate
        eindex=index
        break
      end
    }
    if eindex==1
      eindex=0
    end

    # Build a hash of all project->emp project mappings
    empproj=Hash.new
    IO.foreach("/usr/local/fsl/etc/resource_control") { |line|
      if line=~/^\s*(\S+)\.empproj\s*=\s*(\S+)\s*$/
        empproj[$1]=$2.downcase
      end
    }

    # Initialize user and project and emp hashes
    overall_stats=Hash.new
    
    # Loop over relevant accounting files and read them backwards to accumulate stats
    sindex.downto(eindex) { |index|
      file=files[index]
      if file=~/\.gz$/
        # Uncompress the accounting log if necessary
        fd=IO.popen("gunzip -c #{file} | tac 2>&1")
      else
        fd=IO.popen("tac #{file} 2>&1")
      end
      # Read each line of the tac pipe
      fd.each { |line|
        next if line=~/^#/
        fields=line.split(/:/)
        
        # Skip records that don't fall within the requested time range
        next if fields[10].to_i < ssecs
        next if fields[10].to_i > esecs

        # Skip records for Jet project accounts not specified
        unless accounts.nil?
          next if accounts.index(fields[6]).nil?
        end

        # Skip records for user not specified
        unless users.nil?
          next if users.index(fields[3]).nil?
        end
     
        # Get the relevant stats from the record
        user=fields[3]
        project=fields[6]
        emp=empproj[project]
        emp="unknown" if emp.nil?
        emp="unknown" if emp=~/^UNK$/i
        ncpus=fields[34].to_i
        walltime=fields[13].to_i
        cputime=ncpus*walltime
  
        # Allocate stat hash
        if overall_stats[emp].nil?
          overall_stats[emp]=Hash.new
          overall_stats[emp][project]=Hash.new
          overall_stats[emp][project][user]=Hash.new
          overall_stats[emp][project][user]["njobs"]=0
          overall_stats[emp][project][user]["ncpus"]=0
          overall_stats[emp][project][user]["walltime"]=0
          overall_stats[emp][project][user]["cputime"]=0
        elsif overall_stats[emp][project].nil?
          overall_stats[emp][project]=Hash.new
          overall_stats[emp][project][user]=Hash.new
          overall_stats[emp][project][user]["njobs"]=0
          overall_stats[emp][project][user]["ncpus"]=0
          overall_stats[emp][project][user]["walltime"]=0
          overall_stats[emp][project][user]["cputime"]=0
        elsif overall_stats[emp][project][user].nil?
          overall_stats[emp][project][user]=Hash.new
          overall_stats[emp][project][user]["njobs"]=0
          overall_stats[emp][project][user]["ncpus"]=0
          overall_stats[emp][project][user]["walltime"]=0
          overall_stats[emp][project][user]["cputime"]=0
        end

        # Accumulate stats
        overall_stats[emp][project][user]["njobs"]+=1
        overall_stats[emp][project][user]["ncpus"]+=ncpus
        overall_stats[emp][project][user]["walltime"]+=walltime
        overall_stats[emp][project][user]["cputime"]+=cputime

      }
    }

    stats=Array.new
    overall_stats.keys.sort.each { |emp_key|
      overall_stats[emp_key].keys.sort.each { |project_key|
        overall_stats[emp_key][project_key].keys.sort.each { |user_key|
          stats.push(([emp_key,project_key,user_key] + 
                      [overall_stats[emp_key][project_key][user_key]["njobs"]] + 
                      [overall_stats[emp_key][project_key][user_key]["ncpus"]] + 
                      [overall_stats[emp_key][project_key][user_key]["walltime"]] + 
                      [overall_stats[emp_key][project_key][user_key]["cputime"]]).join(":") )
        }
      }
    }

    return stats.join("%")
   

    # Format the stats into strings with each stat separated by :'s and each user/project/emp seperated by $'s
    total_stats=[total_njobs,total_ncpus,total_walltime,total_cputime].join(":")
    user_stats=user_njobs.keys.sort.collect { |user|
      [user,user_njobs[user],user_ncpus[user],user_walltime[user],user_cputime[user]].join(":")
    }.join("$")
    project_stats=project_njobs.keys.sort.collect { |project|
      [project,project_njobs[project],project_ncpus[project],project_walltime[project],project_cputime[project]].join(":")
    }.join("$")
    emp_stats=emp_njobs.keys.sort.collect { |emp|
      [emp,emp_njobs[emp],emp_ncpus[emp],emp_walltime[emp],emp_cputime[emp]].join(":")
    }.join("$")

    # return a string formatted with each stat type seperated by %'s
    return [total_stats,user_stats,project_stats,emp_stats].join("%")

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
    state=get_job_state(jid.to_i)
    if verbose 
      puts "#{Time.now} :: Job #{jid.to_i} is in state '#{state}'"
    end
    while (state != "r" && state != "done" && Time.now < expire_time)
      if expire_time - Time.now > interval
        sleep interval
      else
        sleep(expire_time - Time.now)
      end
      state=get_job_state(jid.to_i)
      if verbose 
        puts "#{Time.now} :: Job #{jid.to_i} is in state '#{state}'"
      end
    end

    # Raise an exception if the timeout expired
    if (state != "r" && state != "done")
      if verbose 
        puts "#{Time.now} :: Timeout expired!"
      end
      raise TimeoutExpired,"Job #{jid.to_i} did not start in time"
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
    state=get_job_state(jid.to_i)
    if verbose 
      puts "#{Time.now} :: Job #{jid.to_i} is in state '#{state}'"
    end
    while (state != "done" && Time.now < expire_time)
      if expire_time - Time.now > interval
        sleep interval
      else
        sleep(expire_time - Time.now)
      end
      state=get_job_state(jid.to_i)
      if verbose 
        puts "#{Time.now} :: Job #{jid.to_i} is in state '#{state}'"
      end
    end

    # If the timeout has expired, raise an exception
    if (state != "done")
      if verbose 
        puts "#{Time.now} :: Timeout expired!"
      end
      raise TimeoutExpired,"Job #{jid.to_i} did not finish in time"
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

      # Set the SGE_ROOT 
      ENV['SGE_ROOT']=@sge_root

      # Build the submit command, use a wrapper if one exists
      if File.exists?(@@qsub_wrapper)
        cmd=@@qsub_wrapper
      else
        cmd="#{@sge_path}/qsub"
      end
      attributes.each { |attr,value|
        cmd=cmd+" #{attr} #{value}"
      } 
      cmd=cmd+" #{script}"

      # Issue the submit command
      Debug::message("    Running '#{cmd} 2>&1'",1)

      output=Command.run("#{cmd} 2>&1")
      if output[1] != 0
        raise "#{output[0]}"
      end

      # Check for success
      if (output[0]=~/[Yy]our job (\d+) .* has been submitted/)
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

      # Set the SGE_ROOT 
      ENV['SGE_ROOT']=@sge_root

      # Run qdel to delete the job
      output=Command.run("#{@sge_path}/qdel #{jid.to_i}")
      if output[1] != 0
        raise output[0]
      end
      return 0

    rescue
      puts "ERROR: #{@sge_path}/qdel #{jid.to_i} failed"
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
#      output=Command.run("#{@sge_path}/qconf -sss")
#      if output[1] != 0
#        raise output[0]
#      end
#      server=output[0].split(".")[0].chomp

      # Get our hostname
#      host=`hostname -s`.chomp

      # Don't rollover unless we are on the current server
#      return 1 unless host==server

      # Get a list of accounting files sorted by modification time in reverse order
      files=Dir["#{@acct_path}/accounting*"].sort! { |a,b|
        File.stat(b).mtime <=> File.stat(a).mtime
      }

      # Get the date of the first record in the accounting file
      end_time=-1
      file=File.new(files[0])
      file.each { |line|
        next if line=~/^#/
        end_time_field=line.split(":")[10].to_i
        next if end_time_field==0
        end_time=Time.at(end_time_field)
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

      # Get up to date list of files
      files=Dir["#{@acct_path}/accounting*"]

      # Gzip files older than 1 day
      files.reject { |file|
        file=~/\.gz$/ || (Time.now - File.stat(file).mtime < 60*60*24)
      }.each { |file|
        `/bin/gzip #{file}`
      }

#      # Get gzipped files
#      files=Dir["#{@acct_path}/accounting*.gz"]

#      # Copy files to /home for safe keeping
#      homedir="/home/admin/accounting/logs"
#      files.reject { |file|
#        File.exists?("#{homedir}/File.basename(file)")
#      }.each { |file|
#        `/bin/cp -pd #{file} #{homedir}/#{File.basename(file)}`
#      }

      # Return success
      return 0

    rescue
      puts "ERROR: Rollover failed"
      puts $!
      return 1
    end
  end

end

$__sgebatchsystem__ == __FILE__
end
