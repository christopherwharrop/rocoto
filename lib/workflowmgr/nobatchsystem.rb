###########################################
#
# Module WorkflowMgr
#
##########################################
module WorkflowMgr

  require 'workflowmgr/batchsystem'

  ##########################################
  #
  # Class TORQUEBatchSystem
  #
  ##########################################
  class NOBatchSystem < BatchSystem

    BATCH_HELPER= <<-'EOT'
       signal() { echo "FAIL 1 $$" >> "$rocoto_pid_dir"/"$rocoto_jobid" ; exit 1 }
       exitN() { n=$? ; if [[ "$n" == 0 ]] ; then 
         'echo PID 
       'echo PID $$ > "$rocoto_pid_dir"/"$rocoto_jobid"
       
    EOT

    require 'etc'
    require 'parsedate'
    require 'libxml'
    require 'workflowmgr/utilities'
    require 'tempfile'

    #####################################################
    #
    # initialize
    #
    #####################################################
    def initialize(torque_root=nil)

      # Initialize an empty hash for job queue records
      @jobqueue={}

    end


    #####################################################
    #
    # statuses
    #
    #####################################################
    def statuses(jobids)

      begin

        # Initialize statuses to UNAVAILABLE
        jobStatuses={}
        jobids.each do |jobid|
          jobStatuses[jobid] = { :jobid => jobid, :state => "UNAVAILABLE", :native_state => "Unavailable" }
        end

        jobids.each do |jobid|
          jobStatuses[jobid] = self.status(jobid)
        end

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
      
      # We didn't find the job, so return an uknown status record
      return { :jobid => jobid, :state => "UNKNOWN", :native_state => "Unknown" }

    end

    #####################################################
    #
    # make_rocoto_jobid
    #
    #####################################################
    def make_rocoto_jobid()
      # We need a unique jobid.  We'll use a nice, long, string based
      # on the current time in microseconds and some random numbers.
      # This will be a base64 string up to 22 characters in length.
      now_in_usec=Time.now.tv_sec*1e6 + Time.now.tv_usec
      big_hex_number='%015x'%(rand(2**64) ^ now_in_usec)
      return Base64.encode64(big_hex_number).strip().gsub('=','')
    end

    #####################################################
    #
    # submit
    #
    #####################################################
    def submit(task)

      # Initialize the submit command
      cmd=['nohup','setsid','/usr/bin/env']
      rocoto_jobid=make_rocoto_jobid
      cmd += ["rocoto_jobid=#{rocoto_jobid}",
              "rocoto_pid_dir=#{rocoto_pid_dir}",
              'sh', '-c', BATCH_HELPER, "rocoto_bh_#{rocoto_jobid}" ]


      # Default values for shell execution bits: no stdout, stdin,
      # stderr, nor any special env vars.
      stdout_file='/dev/null'
      stdin_file='/dev/null'
      stderr_file='/dev/null'
      set_these_vars={}
      job_name="{rocoto_job_#{rocoto_jobid}"

      # Add Torque batch system options translated from the generic options specification
      task.attributes.each do |option,value|
        case option
          when :stdout
            stdout_file=value
          when :stderr
            stderr_file=value
          when :join
            stdout_file=value
            stderr_file=value
          when :jobname
            job_name=value
        end
      end

      # Add export commands to pass environment vars to the job
      unless task.envars.empty?
        task.envars.each { |name,env|
          cmd << "#{name}=#{env}"
        }
      end

      cmd << "rocoto_jobid=#{rocoto_jobid}"

      # <native> are arguments to sh
      task.each_native do |native_line|
        cmd << native_line
      end

      # Stdin, stdout, and stderr are handled within sh:
      if(stdout_file == stderr_file)
        cmd << "\"$@\" < #{stdin_file} > #{stdout_file} 2>&1"
      else
        cmd << "\"$@\" < #{stdin_file} 2> #{stderr_file} 1> {stdout_file}"
      end

      # Job name is the process name ($0)
      cmd << job_name

      # At the end we place the command to run
      cmd << task.attributes[:command]

      WorkflowMgr.stderr("Running #{cmd.join(' ')}",4)

      result=system(cmd)

      if result.nil? or not result:
        return nil,''
      else
        return 
      end        

    end


    #####################################################
    #
    # delete
    #
    # The "jobid" is a process group id.
    #
    #####################################################
    def delete(jobid)

      process.kill(-jobid)

    end


private

    #####################################################
    #
    # refresh_jobqueue
    #
    #####################################################
    def refresh_jobqueue

      begin

        # Get the username of this process
        username=Etc.getpwuid(Process.uid).name

        # Run qstat to obtain the current status of queued jobs
        queued_jobs=""
        errors=""
        exit_status=0
        queued_jobs,errors,exit_status=WorkflowMgr.run4("qstat -x",30)

        # Raise SchedulerDown if the showq failed
        raise WorkflowMgr::SchedulerDown,errors unless exit_status==0

        # Return if the showq output is empty
        return if queued_jobs.empty?

        # Parse the XML output of showq, building job status records for each job
        queued_jobs_doc=LibXML::XML::Parser.string(queued_jobs, :options => LibXML::XML::Parser::Options::HUGE).parse

      rescue LibXML::XML::Error,Timeout::Error,WorkflowMgr::SchedulerDown
        WorkflowMgr.log("#{$!}")
        WorkflowMgr.stderr("#{$!}",3)
        raise WorkflowMgr::SchedulerDown
      end
      
      # For each job, find the various attributes and create a job record
      queued_jobs=queued_jobs_doc.root.find('//Job')
      queued_jobs.each { |job|

        # Initialize an empty job record
  	record={}

  	# Look at all the attributes for this job and build the record
	job.each_element { |jobstat| 
        
          case jobstat.name
            when /Job_Id/
              record[:jobid]=jobstat.content.split(".").first
            when /job_state/
              case jobstat.content
                when /^Q$/,/^H$/,/^W$/,/^S$/,/^T$/
    	          record[:state]="QUEUED"
                when /^R$/,/^E$/
    	          record[:state]="RUNNING"
                else
                  record[:state]="UNKNOWN"
              end
              record[:native_state]=jobstat.content
            when /Job_Name/
	      record[:jobname]=jobstat.content
	    when /Job_Owner/
	      record[:user]=jobstat.content
            when /Resource_List/       
              jobstat.each_element { |e|
                if e.name=='procs'
                  record[:cores]=e.content.to_i
                  break
                end
            }
  	    when /queue/
	      record[:queue]=jobstat.content
	    when /qtime/
	      record[:submit_time]=Time.at(jobstat.content.to_i).getgm
  	    when /start_time/
              record[:start_time]=Time.at(jobstat.content.to_i).getgm
	    when /comp_time/
              record[:end_time]=Time.at(jobstat.content.to_i).getgm
 	    when /Priority/
	      record[:priority]=jobstat.content.to_i            
            when /exit_status/
              record[:exit_status]=jobstat.content.to_i
	    else
              record[jobstat.name]=jobstat.content
          end  # case jobstat
  	}  # job.children

        # If the job is complete and has an exit status, change the state to SUCCEEDED or FAILED
        if record[:state]=="UNKNOWN" && !record[:exit_status].nil?
          if record[:exit_status]==0
            record[:state]="SUCCEEDED"
          else
            record[:state]="FAILED"
          end
        end

        # Put the job record in the jobqueue unless it's complete but doesn't have a start time, an end time, and an exit status
        unless record[:state]=="UNKNOWN" || ((record[:state]=="SUCCEEDED" || record[:state]=="FAILED") && (record[:start_time].nil? || record[:end_time].nil?))
          @jobqueue[record[:jobid]]=record
        end

      }  #  queued_jobs.find

      queued_jobs=nil

    end  # job_queue

    def process_monitor ( stdin_path, stdout_path, stderr_path, set_these_vars, execute_me )
      # Fork a daemon process.
      fork do
        STDIN=IO.new(0)
        STDOUT=IO.new(1)
        STDERR=IO.new(2)

        STDIN.close
        STDOUT.close
        STDERR.close

        
      end
    end

  end  # class

end  # module

