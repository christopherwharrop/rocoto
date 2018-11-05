###########################################
#
# Module WorkflowMgr
#
##########################################
module WorkflowMgr

  require 'workflowmgr/batchsystem'

  ##########################################
  #
  # Class COBALTBatchSystem
  #
  ##########################################
  class COBALTBatchSystem < BatchSystem

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
    def initialize(cobalt_root=nil)

      # Initialize an empty hash for job queue records
      @jobqueue={}

      # Initialize an empty hash for job accounting records
      @jobacct={}

      # Assume the scheduler is up
      @schedup=true

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

      begin

        raise WorkflowMgr::SchedulerDown unless @schedup

        # Populate the jobs status table if it is empty
        refresh_jobqueue if @jobqueue.empty?

        # Return the jobqueue record if there is one
        return @jobqueue[jobid] if @jobqueue.has_key?(jobid)

        # Populate the job accounting log table
        refresh_jobacct(jobid)

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
      cmd="qsub --debuglog #{ENV['HOME']}/.rocoto/tmp/\\$jobid.log"
      input="#!/bin/sh\n"

      # Add Cobalt batch system options translated from the generic options specification
      task.attributes.each do |option,value|
         if value.is_a?(String)
           if value.empty?
             WorkflowMgr.stderr("WARNING: <#{option}> has empty content and is ignored", 1)
             next
           end
        end
        case option
          when :account
            input += "#COBALT -A #{value}\n"
          when :queue            
            input += "#COBALT -q #{value}\n"
          when :partition
            WorkflowMgr.stderr("WARNING: the <partition> tag is not supported for Cobalt.", 1)
            WorkflowMgr.log("WARNING: the <partition> tag is not supported for Cobalt.", 1)
          when :cores
            # Ignore this attribute if the "nodes" attribute is present
            next unless task.attributes[:nodes].nil?
            WorkflowMgr.stderr("WARNING: Cobalt does not support the <cores> used by task #{task.attributes[:name]}.  Use <nodes> instead.")
          when :nodes
            # Can't support complex geometry in qsub (only in runjob)
            # Compute number of nodes to request
            numnodes = value.split("+").collect { |n| n.split(":ppn=")[0] }.inject { |sum,i| sum.to_i + i.to_i }
            input += "#COBALT -n #{numnodes}\n"
          when :walltime
            minutes = (WorkflowMgr.ddhhmmss_to_seconds(value) / 60.0).ceil
            input += "#COBALT -t #{minutes}\n"
          when :memory
            # Cobalt does not support any way to specify this option
            WorkflowMgr.stderr("WARNING: Cobalt does not support the option <memory> used by task #{task.attributes[:name]}.  It will be ignored.")
          when :stdout
            input += "#COBALT -o #{value}\n"
          when :stderr
            input += "#COBALT -e #{value}\n"
          when :join
            input += "#COBALT -o #{value}\n"
            input += "#COBALT -e #{value}\n"
          when :jobname
            input += "#COBALT --jobname #{value}\n"
        end
      end

      task.each_native do |native_line|        
        next if native_line.empty?
        input += "#COBALT #{native_line}\n"
      end

      # Add export commands to pass environment vars to the job
      unless task.envars.empty?
        varinput=''
        task.envars.each { |name,env|
          varinput += "export #{name}='#{env}'\n"
        }
        input += varinput
      end

      # Add the command to the job
      input += task.attributes[:command]

      # Get a temporary file name to use as a wrapper and write job spec into it
      tfname=Tempfile.new('qsub.in').path.split("/").last
      tf=File.new("#{ENV['HOME']}/.rocoto/tmp/#{tfname}","w")
      tf.write(input)
      tf.flush()
      tf.chmod(0700)
      tf.close
          
      WorkflowMgr.stderr("Submitting #{task.attributes[:name]} using #{cmd} --mode script #{tf.path} with input {{#{input}}}",4)

      # Run the submit command
      output=`#{cmd} --mode script #{tf.path} 2>&1`.chomp()

      # Parse the output of the submit command
      if output=~/^(\d+)$/
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

      qdel=`qdel #{jobid}`      

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
        queued_jobs,errors,exit_status=WorkflowMgr.run4("qstat -l -f -u #{username} ",30)

        # Raise SchedulerDown if the showq failed
        raise WorkflowMgr::SchedulerDown,errors unless exit_status==0

        # Return if the showq output is empty
        return if queued_jobs.empty?

      rescue Timeout::Error,WorkflowMgr::SchedulerDown
        WorkflowMgr.log("#{$!}")
        WorkflowMgr.stderr("#{$!}",3)
        raise WorkflowMgr::SchedulerDown
      end
      
      # For each job, find the various attributes and create a job record
      record = {}
      queued_jobs.split(/\n/).each { |job|

        case job
          when /JobID: (\d+)/
            record = {:jobid => $1}
          when /State\s+:\s+(\S+)/
            record[:native_state]=$1
            case record[:native_state]
              when /running/,/starting/,/exiting/,/killing/
                record[:state] = "RUNNING"
              when /queued/,/hold/
                record[:state] = "QUEUED"
            end
          when /JobName\s+:\s+(\S+)/
            record[:jobname] = $1
          when /User\s+:\s+(\S+)/
            record[:user] = $1
          when /Procs\s+:\s+(\d+)/
            record[:cores] = $1.to_i
          when /Queue\s+:\s+(\S+)/
            record[:queue] = $1
          when /SubmitTime\s+:\s+(\S+.*)/
            record[:submit_time] = Time.gm(*ParseDate.parsedate($1))
          when /StartTime\s+:\s+(\S+.*)/
            record[:start_time] = Time.gm(*ParseDate.parsedate($1)) unless $1 == "N/A"
          when /(\S+)\s+:\s+(\S+)/
            record[$1] = $2
        end

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


    #####################################################
    #
    # refresh_jobacct
    #
    #####################################################
    def refresh_jobacct(jobid)

      # Get the username of this process
      username=Etc.getpwuid(Process.uid).name

      begin

        joblogfile = "#{ENV['HOME']}/.rocoto/tmp/#{jobid}.log"
        return unless  File.exists?(joblogfile)
        joblog = IO.readlines(joblogfile,nil)[0]

        # Return if the joblog output is empty
        return if joblog.empty?

      rescue WorkflowMgr::SchedulerDown
        WorkflowMgr.log("#{$!}")
        WorkflowMgr.stderr("#{$!}",3)
        raise WorkflowMgr::SchedulerDown        
      end 

      # For each job, find the various attributes and create a job record
      record={:jobid => jobid}
      joblog.split(/\n/).each { |line|

        case line
          when /submitted with cwd set to:/
            record[:submit_time]=Time.gm(*ParseDate.parsedate(line))
          when /(\S+)\/\d+:\s+Initiating boot at location/
            record[:user]=$1
            record[:start_time]=Time.gm(*ParseDate.parsedate(line))
          when /task completed normally with an exit code of (\d+);/
            record[:end_time]=Time.gm(*ParseDate.parsedate(line))
            record[:exit_status] = $1.to_i
          when /^qsub.* (\S+)$/
            record[:command] = $1
          when /Info: user delete requested/
            record[:exit_status] = 255
            record[:native_state] = "deleted"           
          when /initiating job termination/
            record[:exit_status] = 255
            record[:end_time]=Time.gm(*ParseDate.parsedate(line))
            record[:native_state]="killed"                        
        end

      }

      unless record[:start_time].nil? || record[:end_time].nil?
        record[:duration] = record[:end_time] - record[:start_time]
      end

      if record[:exit_status].nil?
        record[:state]="UNKNOWN"
        record[:exit_status]=255
      elsif record[:exit_status]==0
        record[:state]="SUCCEEDED"
      else
        record[:state]="FAILED"
      end
      if record[:state]=="UNKNOWN"
        record[:native_state]="unknown"
      else
        case record[:native_state]
          when "deleted"
            record[:start_time] = Time.at(0)
            record[:end_time] = Time.at(0)
            record[:duration] = 0
          when "killed"
            # Do nothing
          else
            record[:native_state]="completed"
        end
      end

      # Add the record if it hasn't already been added
      unless @jobacct.has_key?(record[:jobid])
        @jobacct[record[:jobid]]=record
        # Remove the temporary submit script
        FileUtils.rm(record[:command])
        # Remove the temporary cobaltlog
        FileUtils.rm(joblogfile)        
      end

    end

  end  # class

end  # module

