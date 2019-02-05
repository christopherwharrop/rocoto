##########################################
#
# Module WorkflowMgr
#
##########################################
module WorkflowMgr

  require 'workflowmgr/batchsystem'

  ##########################################  
  #
  # Class SGEBatchSystem 
  #
  ##########################################
  class SGEBatchSystem < BatchSystem

    ###############################################################################
    #
    # SGE accounting record format (fields are separated by colons)
    #
    #  0    qname		Name of the queue in which the job has run.
    # 
    #  1    hostname		Name of the execution host.
    # 
    #  2    group  		The effective group id of the job owner when 
    #				executing the job.
    # 
    #  3    owner  		Owner of the Grid Engine job.
    # 
    #  4    job_name		Job name.
    # 
    #  5    job_number      	Job identifier - job number.
    # 
    #  6    account         	An account string as specified by the qsub(1) or 
    #				qalter(1) -A option.
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

    require 'etc'
    require 'libxml'

    #####################################################
    #
    # initialize
    #
    #####################################################
    def initialize(sge_root=nil)

      # Get/Set the SGE_ROOT
      if sge_root.nil?
	raise "Cannot initialize SGE batch system object.  $SGE_ROOT is undefined!" if ENV['SGE_ROOT'].nil?
      else
	ENV['SGE_ROOT']=sge_root
      end

      # Get the SGE architecture
      @sge_arch=`#{ENV['SGE_ROOT']}/util/arch`.chomp

      # Set the path to the SGE commands
      @sge_bin="#{ENV['SGE_ROOT']}/bin/#{@sge_arch}"

      # Find the path to the default SGE accounting log
      @sge_acct="#{ENV['SGE_ROOT']}/default/common/accounting"

      # Find the directory containing the default SGE accounting log
      catch (:done) do
	linkcount=0
	loop do
	  if File.symlink?(@sge_acct)
	    @sge_acct=File.expand_path(File.readlink(@sge_acct),@sge_acct)
	    linkcount+=1
	  else
	    throw :done
	  end
	  raise "Cannot initialize SGE batch system object.  Too many links in accounting record path!" if linkcount > 100
	end
      end
      @sge_acct=File.dirname(@sge_acct)


      # Initialize an empty hash for job queue records
      @jobqueue={}

      # Initialize an empty hash for job accounting records
      @jobacct={}

      # Initialize the hrs back contained in the jobacct hash
      @hrsback=0    

    end


    #####################################################
    #
    # statuses
    #
    #####################################################
    def statuses(jobids)

      begin

        raise WorkflowMgr::SchedulerDown unless @schedup

        # Initialize statuses to UNAVAILABLE
        jobStatuses={}
        jobids.each do |jobid|
          jobStatuses[jobid] = { :jobid => jobid, :state => "UNAVAILABLE", :native_state => "Unavailable" }
        end

        jobids.each do |jobid|
          jobStatuses[jobid] = self.status(jobid)
        end

      rescue WorkflowMgr::SchedulerDown
        @schedup=false
      ensure
        return jobStatuses
      end

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
      return { :jobid => jobid, :state => "UNKNOWN", :native_state => "unknown" }

    end

    #####################################################
    #
    # submit
    #
    #####################################################
    def submit(task)

      # Initialize the submit command
      cmd="qsub"

      # Add SGE batch system options translated from the generic options specification
      task.attributes.each do |option,value|
         if value.is_a?(String)
           if value.empty?
             WorkflowMgr.stderr("WARNING: <#{option}> has empty content and is ignored", 1)
             next
           end
        end
        case option
          when :account
            cmd += " -A #{value}"
          when :queue            
            cmd += " -q #{value}"
          when :partition
            unless cmd =~/ -pe \S+ \d+/
              cmd += " -pe #{value} #{task.attributes[:cores]}"
            end
          when :cores
            unless cmd =~/ -pe \S+ \d+/
              cmd += " -pe #{task.attributes[:partition]} #{value}"
            end           
          when :nodes
            WorkflowMgr.stderr("WARNING: the <partition> tag is not supported for SGE.", 1)
            WorkflowMgr.log("WARNING: the <partition> tag is not supported for SGE.", 1)
          when :walltime
            hhmmss=WorkflowMgr.seconds_to_hhmmss(WorkflowMgr.ddhhmmss_to_seconds(value))
            cmd += " -l h_rt=#{hhmmss}"
          when :memory
            cmd += " -l h_vmem=#{value}"
          when :stdout
            cmd += " -o #{value}"
          when :stderr
            cmd += " -e #{value}"
          when :join
            cmd += " -o #{value} -j y"           
          when :jobname
            cmd += " -N #{value}"
        end
      end

      task.each_native do |native_line|
        cmd += " #{native_line}"
      end

      # Add environment vars
      unless task.envars.empty?
        vars = "" 
        task.envars.each { |name,env|
          if vars.empty?
            vars += " -v #{name}"
          else
            vars += ",#{name}"
          end
          vars += "=\"#{env}\"" unless env.nil?
        }
        cmd += "#{vars}"
      end

      # Add the command to submit
      cmd += " #{task.attributes[:command]}"
      WorkflowMgr.stderr("Submitting #{task.attributes[:name]} using '#{cmd}'",4)

      # Run the submit command
      output=`#{cmd} 2>&1`.chomp

      # Parse the output of the submit command
      if output=~/Your job (\d+) \(".*"\) has been submitted/
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

      qdel=`#{@sge_bin}/qdel -j #{jobid}`      

    end


  private

    #####################################################
    #
    # refresh_jobqueue
    #
    #####################################################
    def refresh_jobqueue

      # Run qstat to obtain the current status of queued jobs
      queued_jobs=`#{@sge_bin}/qstat -xml -u \\*`

      # Return if the output of qstat is empty
      return if queued_jobs.empty?

      # Parse the XML output of the qstat, building job status records for each job
      queued_jobs_doc=LibXML::XML::Parser.string(queued_jobs, :options => LibXML::XML::Parser::Options::HUGE).parse

      # For each job, find the various attributes and create a job record
      queued_jobs=queued_jobs_doc.root.find('//job_list')
      queued_jobs.each { |jobsearch|

        job=jobsearch.copy("deep")

	# Initialize an empty job record
	record={}

	# Look at all the attributes for this job and build the record
	job.children.each { |jobstat| 
	  if jobstat.element?
	    case jobstat.name
	      when /JB_job_number/
		record[:jobid]=jobstat.content
	      when /state/
                case jobstat.content
                  when /^qw$/
    	            record[:state]="QUEUED"
                  when /^[R]*r$/,/^t$/
    	            record[:state]="RUNNING"
                  when /^E/
    	            record[:state]="ERROR"
                  else
    	            record[:state]="UNKNOWN"
                end
		record[:native_state]=jobstat.content
	      when /JB_name/
		record[:jobname]=jobstat.content
	      when /JB_owner/
		record[:user]=jobstat.content
	      when /slots/
		record[:cores]=jobstat.content.to_i
	      when /queue_name/
		record[:queue]=jobstat.content
	      when /JB_submission_time/
		record[:submit_time]=Time.local(*DateTime.parse(jobstat.content).strftime("%Y %m %d %H %M %S").split).getgm
	      when /JAT_start_time/
		record[:start_time]=Time.local(*DateTime.parse(jobstat.content).strftime("%Y %m %d %H %M %S").split).getgm
	      when /JAT_prio/
		record[:priority]=jobstat.content.to_f
	      else
		record[jobstat.name]=jobstat.content
	    end  # case jobstat
	  end  # if jobstat.element?
	}  # job.children

	# Put the job record in the jobqueue
	@jobqueue[record[:jobid]]=record

      }  #  queued_jobs.find

      queued_jobs=nil

    end


    #####################################################
    #
    # refresh_jobacct
    #
    #####################################################
    def refresh_jobacct(hrsback=1)

      # Get the username of this process
      username=Etc.getpwuid(Process.uid).name

      # Get a list of the accounting files modified 24 hours or less ago
      cutoff_time=Time.now - hrsback * 3600
      acct_files=Dir["#{@sge_acct}/accounting*"].delete_if { |acct_file| File.mtime(acct_file) <  cutoff_time }

      # Sort the list of accounting files in reverse order by modification time
      acct_files.sort! { |a,b| File.mtime(b) <=> File.mtime(a) }

      # Initialize an empty hash of job records
      @jobacct={}

      # Read the accounting files backwards until we read 24 hours worth of data
      catch(:done) do

        acct_files.each { |acct_file|

          # Select the command to read the file, depending on whether it is compressed or not
          if acct_file=~/\.gz$/
            cmd="gunzip -c #{acct_file} | tac "
	  else
	    cmd="tac #{acct_file}"
	  end

	  # Open a pipe to the command that reads the file backward
	  IO.popen(cmd,"r") {|pipe|

	    # Keep reading until we hit the end of the file
	    while !pipe.eof?

	      # Read a record and split it into fields
	      record=pipe.gets
	      fields=record.split(/:/)

	      # Skip bogus records
	      next unless fields.length >= 43 and fields.length <= 45  # Wrong number of fields
	      next unless fields[10].to_i > 0                          # Invalid completion time

	      # Quit if we've reached the minimum completion time
	      throw :done if Time.at(fields[10].to_i) < cutoff_time

	      # Skip records for other users' jobs
	      next unless fields[3]==username

	      # Skip bogus records
	      next unless fields[8].to_i > 0                           # Invalid submit time

	      # Extract relevant fields
	      record={}
	      record[:jobid]=fields[5]
	      record[:native_state]="done"
	      record[:jobname]=fields[4]
	      record[:user]=fields[3]
	      record[:cores]=fields[34].to_i
	      record[:queue]=fields[0]
	      record[:submit_time]=Time.at(fields[8].to_i).getgm
	      record[:start_time]=Time.at(fields[9].to_i).getgm
	      record[:end_time]=Time.at(fields[10].to_i).getgm
	      record[:exit_status]=fields[12].to_i==0 ? fields[11].to_i : fields[12].to_i
	      record[:priority]=fields[7].to_f
              if record[:exit_status]==0
                record[:state]="SUCCEEDED"
              else
                record[:state]="FAILED"
              end

	      # Add the record if it hasn't already been added
	      @jobacct[fields[5]]=record unless @jobacct.has_key?(fields[5].to_i)

	    end  # while !pipe.eof?

	  }  # IO.popen

        }  # acct_files.each

      end  # catch :done

      # Update the hrsback if needed
      @hrsback=hrsback if hrsback > @hrsback

    end

  end  # Class SGEBatchSystem

end
