###########################################
#
# Module WorkflowMgr
#
##########################################
module WorkflowMgr

  require 'workflowmgr/batchsystem'

  ##########################################
  #
  # Class MOABBatchSystem
  #
  ##########################################
  class MOABBatchSystem < BatchSystem

    require 'etc'
    require 'parsedate'
    require 'libxml'
    require 'workflowmgr/utilities'
    require 'workflowmgr/torquebatchsystem'

    #####################################################
    #
    # initialize
    #
    #####################################################
    def initialize(moab_root=nil,torque_root=nil)

      # Initialize an empty hash for job queue records
      @jobqueue={}

      # Initialize an empty hash for job accounting records
      @jobacct={}

      # Currently there is no way to specify the amount of time to 
      # look back at finished jobs.  MOAB showq will always return 
      # all the records it finds the first time.  So, set this to 
      # a big value to make sure showq doesn't get run twice.
      @hrsback=120

      # Assume the scheduler is up
      @schedup=true

    end


    #####################################################
    #
    # status
    #
    #####################################################
    def status(jobid)

      begin

        raise WorkflowMgr::SchedulerDown unless @schedup

        # Populate the jobs status table if it is empty
        refresh_jobqueue if @jobqueue.empty?

        # Return the jobqueue record if there is one
        return @jobqueue[jobid] if @jobqueue.has_key?(jobid)

        # Populate the job accounting log table if it is empty
        refresh_jobacct if @jobacct.empty?

        # Return the jobacct record if there is one
        return @jobacct[jobid] if @jobacct.has_key?(jobid)

        # The state is unavailable since Moab doesn't have the state
        return { :jobid => jobid, :state => "UNKNOWN", :native_state => "Unknown" }
 
      rescue WorkflowMgr::SchedulerDown
        @schedup=false
        return { :jobid => jobid, :state => "UNAVAILABLE", :native_state => "Unavailable" }
      end

    end


    #####################################################
    #
    # submit
    #
    #####################################################
    def submit(task)

      # Initialize the submit command
      cmd="msub"
      input="#! /bin/sh\n"

      # Add Torque batch system options translated from the generic options specification
      task.attributes.each do |option,value|
        case option
          when :account
            input += "#PBS -A #{value}\n"
          when :queue            
            input += "#PBS -q #{value}\n"
          when :cores
            # Ignore this attribute if the "nodes" attribute is present
            next unless task.attributes[:nodes].nil?
            input += "#PBS -l procs=#{value}\n"
          when :nodes
            # Replace -l nodes=a:b+c:d+... with a single -l nodes=
            nodes=0
            value.split('+').each { |nodespec|
              resources=nodespec.split(':ppn=')
              nodes=nodes+resources.shift.to_i
            }
            if nodes<1
              nodes=1
            end
            input += "#PBS -l nodes=#{nodes}\n"
          when :walltime
            input += "#PBS -l walltime=#{value}\n"
          when :memory
            input += "#PBS -l vmem=#{value}\n"
          when :stdout
            input += "#PBS -o #{value}\n"
          when :stderr
            input += "#PBS -e #{value}\n"
          when :join
            input += "#PBS -j oe -o #{value}\n"           
          when :jobname
            input += "#PBS -N #{value}\n"
        end
      end

      task.each_native do |native_line|
        input += "#PBS #{native_line}\n"
      end

      # Add export commands to pass environment vars to the job
      unless task.envars.empty?
        varinput=''
        task.envars.each { |name,env|
          varinput += "export #{name}='#{env}'\n"
        }
        input += varinput
      end
      input+="set -x\n"

      # Add the command to execute
      input += task.attributes[:command]

      # Generate the execution script that will be submitted
      tf=Tempfile.new('qsub.in')
      tf.write(input)
      tf.flush()

      WorkflowMgr.stderr("Submitting #{task.attributes[:name]} using #{cmd} < #{tf.path} with input {{#{input}}}",4)

      # Run the submit command
      output=`#{cmd} < #{tf.path} 2>&1`.chomp()

      # Parse the output of the submit command
      if output=~/^((\w+\.)*\d+)$/
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

      qdel=`mjobctl -c #{jobid}`      

    end


private

    #####################################################
    #
    # refresh_jobqueue
    #
    #####################################################
    def refresh_jobqueue

      # Get the username of this process
      username=Etc.getpwuid(Process.uid).name

      begin

        # Run qstat to obtain the current status of queued jobs
        queued_jobs=""
        errors=""
        exit_status=0
        queued_jobs,errors,exit_status=WorkflowMgr.run4("showq --noblock --xml -u #{username}",30)
                
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
      queued_jobs=queued_jobs_doc.find('//job')
      queued_jobs.each { |job|

	# Initialize an empty job record
	record={}

	# Look at all the attributes for this job and build the record
	job.attributes.each { |jobstat| 
          case jobstat.name
            when /JobID/
#              record[:jobid]=jobstat.value.split(".").last
              record[:jobid]=jobstat.value
            when /State/
              case jobstat.value
                when /^Idle$/,/^.*Hold$/,/^Deferred$/
    	          record[:state]="QUEUED"
                when /^Running$/
    	          record[:state]="RUNNING"
                else
    	          record[:state]="UNKNOWN"
              end
	      record[:native_state]=jobstat.value
	    when /JobName/
	      record[:jobname]=jobstat.value
	    when /User/
	      record[:user]=jobstat.value
	    when /ReqProcs/
	      record[:cores]=jobstat.value.to_i
	    when /Class/
	      record[:queue]=jobstat.value
	    when /SubmissionTime/
	      record[:submit_time]=Time.at(jobstat.value.to_i).getgm
	    when /StartTime/
              record[:start_time]=Time.at(jobstat.value.to_i).getgm
	    when /StartPriority/
	      record[:priority]=jobstat.value.to_i            
	    else
              record[jobstat.name]=jobstat.value
          end  # case jobstat
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
    def refresh_jobacct

      # Get the username of this process
      username=Etc.getpwuid(Process.uid).name

      # Initialize an empty hash of job records
      @jobacct={}

      begin

        # Run showq to obtain the current status of queued jobs
        completed_jobs=""
        errors=""
        exit_status=0
        completed_jobs,errors,exit_status=WorkflowMgr.run4("showq -c --noblock --xml -u #{username}",30)

        # Raise SchedulerDown if the showq failed
        raise WorkflowMgr::SchedulerDown,errors unless exit_status==0

        # Return if the showq output is empty
        return if completed_jobs.empty?

        # Parse the XML output of showq, building job status records for each job
        recordxmldoc=LibXML::XML::Parser.string(completed_jobs, :options => LibXML::XML::Parser::Options::HUGE).parse

      rescue LibXML::XML::Error,Timeout::Error,WorkflowMgr::SchedulerDown
        WorkflowMgr.log("#{$!}")
        WorkflowMgr.stderr("#{$!}",3)
        raise WorkflowMgr::SchedulerDown        
      end 

      # For each job, find the various attributes and create a job record
      recordxml=recordxmldoc.find('//job')       
      recordxml.each { |job|

        record={}
#        record[:jobid]=job.attributes['JobID'].split(".").last
        record[:jobid]=job.attributes['JobID']
        record[:native_state]=job.attributes['State']
        record[:jobname]=job.attributes['JobName']
        record[:user]=job.attributes['User']
        record[:cores]=job.attributes['ReqProcs'].to_i
        record[:queue]=job.attributes['Class']
        record[:submit_time]=Time.at(job.attributes['SubmissionTime'].to_i).getgm
        record[:start_time]=Time.at(job.attributes['StartTime'].to_i).getgm
        record[:end_time]=Time.at(job.attributes['CompletionTime'].to_i).getgm
        record[:duration]=job.attributes['AWDuration'].to_i
        record[:priority]=job.attributes['StartPriority'].to_i
        if job.attributes['State']=~/^Removed/ || job.attributes['CompletionCode']=~/^CNCLD/
          record[:exit_status]=255
	else
          record[:exit_status]=job.attributes['CompletionCode'].to_i
        end
        if record[:exit_status]==0
          record[:state]="SUCCEEDED"
        else
          record[:state]="FAILED"
        end

        # Add the record if it hasn't already been added
        @jobacct[record[:jobid]]=record unless @jobacct.has_key?(record[:jobid])

      }

      recordxml=nil
    
    end

  end

end

