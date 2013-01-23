###########################################
#
# Module WorkflowMgr
#
##########################################
module WorkflowMgr

  ##########################################
  #
  # Class TORQUEBatchSystem
  #
  ##########################################
  class TORQUEBatchSystem

    require 'etc'
    require 'parsedate'
    require 'libxml'
    require 'workflowmgr/utilities'

    #####################################################
    #
    # initialize
    #
    #####################################################
    def initialize(torque_root=nil)

      # Initialize an empty hash for job queue records
      @jobqueue={}

      # Initialize an empty hash for job accounting records
      @jobacct={}

      # Currently there is no way to specify the amount of time to 
      # look back at finished jobs. So set this to a big value
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

      # Add Torque batch system options translated from the generic options specification
      task.attributes.each do |option,value|
        case option
          when :account
            cmd += " -A #{value}"
          when :queue            
            cmd += " -q #{value}"
          when :cores
            cmd += " -l procs=#{value}"
          when :walltime
            cmd += " -l walltime=#{value}"
          when :memory
            cmd += " -l vmem=#{value}"
          when :stdout
            cmd += " -o #{value}"
          when :stderr
            cmd += " -e #{value}"
          when :join
            cmd += " -j oe -o #{value}"           
          when :jobname
            cmd += " -N #{value}"
          when :native
	    cmd += " #{value}"
        end
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

      # Add the command arguments
      cmdargs=task.attributes[:command].split[1..-1].join(" ")
      unless cmdargs.empty?
        cmd += " -F \"#{cmdargs}\""
      end

      # Add the command to submit
      cmd += " #{task.attributes[:command].split.first}"
      WorkflowMgr.stderr("Submitted #{task.attributes[:name]} using '#{cmd}'",10)

      # Run the submit command
      output=`#{cmd} 2>&1`.chomp

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
        raise WorkflowMgr::SchedulerDown unless exit_status==0

        # Return if the showq output is empty
        return if queued_jobs.empty?

        # Parse the XML output of showq, building job status records for each job
        queued_jobs_doc=LibXML::XML::Parser.string(queued_jobs).parse

      rescue LibXML::XML::Error,Timeout::Error,WorkflowMgr::SchedulerDown
        WorkflowMgr.log("#{errors}") unless errors.empty?
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
              if record[:exit_status]==0
                record[:state]="SUCCEEDED"
              else
                record[:state]="FAILED"
              end
	    else
              record[jobstat.name]=jobstat.content
          end  # case jobstat
  	}  # job.children

  	# Put the job record in the jobqueue
	@jobqueue[record[:jobid]]=record

      }  #  queued_jobs.find

      queued_jobs=nil
      GC.start

    end  # job_queue

  end  # class

end  # module

