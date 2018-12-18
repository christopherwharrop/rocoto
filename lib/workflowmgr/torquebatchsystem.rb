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
  class TORQUEBatchSystem < BatchSystem

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

      # Currently there is no way to specify the amount of time to 
      # look back at finished jobs. So set this to a big value
      @hrsback=120

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

        # We didn't find the job, so return an uknown status record
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
      cmd="qsub"
      input="#! /bin/sh\n"

      # Add Torque batch system options translated from the generic options specification
      task.attributes.each do |option,value|
        case option
          when :account
            input += "#PBS -A #{value}\n"
          when :queue            
            input += "#PBS -q #{value}\n"
          when :partition
            input += "#PBS -l partition=#{value}\n"
          when :cores
            # Ignore this attribute if the "nodes" attribute is present
            next unless task.attributes[:nodes].nil?
            input += "#PBS -l procs=#{value}\n"
          when :nodes
            # Remove any occurrences of :tpp=N
            input += "#PBS -l nodes=#{value.gsub(/:tpp=\d+/,"")}\n"
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
      if output=~/^(\d+)(\.[a-zA-Z0-9-]+)*$/
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

  end  # class

end  # module

