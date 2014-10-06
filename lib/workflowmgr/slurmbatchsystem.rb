###########################################
#
# Module WorkflowMgr
#
##########################################
module WorkflowMgr

  ##########################################
  #
  # Class SLURMBatchSystem
  #
  ##########################################
  class SLURMBatchSystem

    require 'etc'
    require 'parsedate'
    require 'libxml'
    require 'workflowmgr/utilities'

    #####################################################
    #
    # initialize
    #
    #####################################################
    def initialize(slurm_root=nil)

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
      cmd="sbatch"

      # Add Slurm batch system options translated from the generic options specification
      task.attributes.each do |option,value|
#        case option
#          when :account
#            cmd += " -A #{value}"
#          when :queue            
#            cmd += " -q #{value}"
#          when :cores
#            # Ignore this attribute if the "nodes" attribute is present
#            next unless task.attributes[:nodes].nil?
#            cmd += " -l procs=#{value}"
#          when :nodes
#            # Remove any occurrences of :tpp=N
#            cmd += " -l nodes=#{value.gsub(/:tpp=\d+/,"")}"
#          when :walltime
#            cmd += " -l walltime=#{value}"
#          when :memory
#            cmd += " -l vmem=#{value}"
#          when :stdout
#            cmd += " -o #{value}"
#          when :stderr
#            cmd += " -e #{value}"
#          when :join
#            cmd += " -j oe -o #{value}"           
#          when :jobname
#            cmd += " -N #{value}"
#          when :native
#            cmd += " #{value}"
#        end
      end

      # Build the -v string to pass environment to the job
#      save_env={}
#      unless task.envars.empty?
#        vars = "-v " 
#        task.envars.each { |name,env|
#          if env=~/[\s,-]+/ || vars.length > 2048
#            vars="-V"
#            break
#          end
#          vars += "," unless vars=="-v "
#          vars += "#{name}"
#          vars += "=\"#{env}\"" unless env.nil?
#        }

#        # Choose -v or -V depending on how long -v is
#        if vars=="-V"
#          # Save a copy of the current environment so we can restore it later
#          save_env.merge(ENV) 
#
#          # Set all envars in the current environment so they get passed with -V
#          task.envars.each { |name,env|
#            ENV[name]=env
#          }
#        end          
#        cmd += " #{vars}"
#      end

#      # Build the -F string to pass job script arguments to batch script
#      cmdargs=task.attributes[:command].split[1..-1].join(" ")
#      unless cmdargs.empty?
#        cmd += " -F \"#{cmdargs}\""
#      end

      # Add the command to submit
      cmd += " #{task.attributes[:command].split.first}"
      WorkflowMgr.stderr("Submitting #{task.attributes[:name]} using '#{cmd}'",4)

      # Run the submit command
      output=`#{cmd} 2>&1`.chomp

#      # Restore the environment if necessary
#      unless save_env.empty?
#        ENV.clear
#        save_env.each { |k,v| ENV[k]=v }
#      end

      # Parse the output of the submit command
      if output=~/^Submitted batch job (\d+)$/
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

      qdel=`scancel #{jobid}`      

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
        queued_jobs,errors,exit_status=WorkflowMgr.run4("squeue -u #{username}",30)

        # Raise SchedulerDown if the command failed
        raise WorkflowMgr::SchedulerDown,errors unless exit_status==0

        # Return if the output is empty
        return if queued_jobs.empty?

      rescue Timeout::Error,WorkflowMgr::SchedulerDown
        WorkflowMgr.log("#{$!}")
        WorkflowMgr.stderr("#{$!}",3)
        raise WorkflowMgr::SchedulerDown
      end

      # For each job, find the various attributes and create a job record
      queued_jobs.split("\n").each { |job|

        # Initialize an empty job record
  	record={}

  	# Look at all the attributes for this job and build the record
	jobfields=job.strip.split(/\s+/)

        # Extract job id        
        record[:jobid]=jobfields[0]

        # Extract job state
        case jobfields[4]
          when /^Q$/,/^H$/,/^W$/,/^S$/,/^T$/
            record[:state]="QUEUED"
          when /^R$/,/^E$/
            record[:state]="RUNNING"
          else
            record[:state]="UNKNOWN"
        end
        record[:native_state]=jobfields[4]

#            when /Job_Name/
#	      record[:jobname]=jobstat.content
#	    when /Job_Owner/
#	      record[:user]=jobstat.content
#            when /Resource_List/       
#              jobstat.each_element { |e|
#                if e.name=='procs'
#                  record[:cores]=e.content.to_i
#                  break
#                end
#            }
#  	    when /queue/
#	      record[:queue]=jobstat.content
#	    when /qtime/
#	      record[:submit_time]=Time.at(jobstat.content.to_i).getgm
#  	    when /start_time/
#              record[:start_time]=Time.at(jobstat.content.to_i).getgm
#	    when /comp_time/
#              record[:end_time]=Time.at(jobstat.content.to_i).getgm
# 	    when /Priority/
#	      record[:priority]=jobstat.content.to_i            
#            when /exit_status/
#              record[:exit_status]=jobstat.content.to_i
#	    else
#              record[jobstat.name]=jobstat.content
#          end  # case jobstat
#  	}  # job.children

#        # If the job is complete and has an exit status, change the state to SUCCEEDED or FAILED
#        if record[:state]=="UNKNOWN" && !record[:exit_status].nil?
#          if record[:exit_status]==0
#            record[:state]="SUCCEEDED"
#          else
#            record[:state]="FAILED"
#          end
#        end

        # Put the job record in the jobqueue unless it's complete but doesn't have a start time, an end time, and an exit status
        unless record[:state]=="UNKNOWN" || ((record[:state]=="SUCCEEDED" || record[:state]=="FAILED") && (record[:start_time].nil? || record[:end_time].nil?))
          @jobqueue[record[:jobid]]=record
        end

      }  #  queued_jobs.find

#      queued_jobs=nil
#      GC.start

    end  # job_queue

  end  # class

end  # module

