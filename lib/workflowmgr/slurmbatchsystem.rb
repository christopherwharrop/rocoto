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
        case option
          when :account
            cmd += " --account #{value}"
          when :queue            
            cmd += " -p #{value}"
          when :cores
            # Ignore this attribute if the "nodes" attribute is present
            next unless task.attributes[:nodes].nil?
            cmd += " --ntasks=#{value}"
          when :nodes
            # Arbitrary processor geometry is not support by sbatch (it is supported by srun)
            # Request number of nodes and tasks per node large enough to accommodate nodespec
            # Get the total number of nodes and the maximum ppn
            maxppn=1
            nnodes=0
            nodespecs=value.split("+")
            nodespecs.each { |nodespec|
              resources=nodespec.split(":")
              nnodes+=resources.shift.to_i
              ppn=0
              resources.each { |resource|
                case resource
                  when /ppn=(\d+)/
                    ppn=$1.to_i
                  when /tpp=(\d+)/
                    tpp=$1.to_i
                end
              }
              maxppn=ppn if ppn > maxppn
            }

            # Request total number of nodes
            cmd += " --nodes=#{nnodes}-#{nnodes}"

            # Request max tasks per node
            cmd += " --tasks-per-node=#{maxppn}"

            # Make sure exclusive access to nodes is enforced
            cmd += " --exclusive"

            # Print a warning if multiple nodespecs are specified
            if nodespecs.size > 1
              WorkflowMgr.stderr("WARNING: SLURM does not support multiple types of node requests for batch jobs",1)
              WorkflowMgr.stderr("WARNING: You must use the -m option of the srun command in your script to launch your code with an arbitrary distribution of tasks",1)
              WorkflowMgr.stderr("WARNING: Please see https://computing.llnl.gov/linux/slurm/faq.html#arbitrary for details",1)
              WorkflowMgr.stderr("WARNING: Rocoto has automatically converted '#{value}' to '#{nnodes}:ppn=#{maxppn}' to facilitate the desired arbitrary task distribution",1)
            end

           when :walltime
            # Make sure format is dd-hh:mm:ss if days are included
            cmd += " -t #{value.sub(/^(\d+):(\d+:\d+:\d+)$/,'\1-\2')}"
          when :memory
            cmd += " --mem #{value}"
          when :stdout
            cmd += " -o #{value}"
          when :stderr
            cmd += " -e #{value}"
          when :join
            cmd += " -o #{value}"           
          when :jobname
            cmd += " --job-name #{value}"
          when :native
            cmd += " #{value}"
        end
      end

      # Build the -v string to pass environment to the job
      save_env={}
      unless task.envars.empty?
        vars = "--export=" 
        task.envars.each { |name,env|
          if env=~/[\s,-]+/ || vars.length > 2048
            vars="--export=ALL"
            break
          end
          vars += "," unless vars=="--export="
          vars += "#{name}"
          vars += "=\"#{env}\"" unless env.nil?
        }

        # Choose -v or -V depending on how long -v is
        if vars=="--export=ALL"
          # Save a copy of the current environment so we can restore it later
          save_env.merge(ENV) 

          # Set all envars in the current environment so they get passed with -V
          task.envars.each { |name,env|
            ENV[name]=env
          }
        end          
        cmd += " #{vars}"
      end

      # Add the command to submit
      cmd += " #{task.attributes[:command]}"
      WorkflowMgr.stderr("Submitting #{task.attributes[:name]} using '#{cmd}'",4)

      # Run the submit command
      output=`#{cmd} 2>&1`.chomp

      # Restore the environment if necessary
      unless save_env.empty?
        ENV.clear
        save_env.each { |k,v| ENV[k]=v }
      end

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
        queued_jobs,errors,exit_status=WorkflowMgr.run4("scontrol -o show job",30)

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
	jobfields=Hash[job.split.collect {|f| f.split("=") }]

        # Skip records for other users
        next unless jobfields["UserId"] =~/^#{username}\(/

        # Extract job id        
        record[:jobid]=jobfields["JobId"]

        # Extract job name
        record[:jobname]=jobfields["Name"]

        # Extract job owner
        record[:user]=jobfields["UserId"].split("(").first

        # Extract core count
        record[:cores]=jobfields["NumCPUs"].to_i

        # Extract the partition
        record[:queue]=jobfields["Partition"]

        # Extract the submit time
        record[:submit_time]=Time.local(*jobfields["SubmitTime"].split(/[-:T]/)).getgm

        # Extract the start time
        record[:start_time]=Time.local(*jobfields["StartTime"].split(/[-:T]/)).getgm

        # Extract the end time
        record[:end_time]=Time.local(*jobfields["EndTime"].split(/[-:T]/)).getgm

        # Extract the priority
        record[:priority]=jobfields["Priority"]

        # Extract the exit status
        code,signal=jobfields["ExitCode"].split(":").collect {|i| i.to_i}
        if code==0
          record[:exit_status]=signal
        else
          record[:exit_status]=code
        end            

        # Extract job state
        case jobfields["JobState"]       
          when /^CONFIGURING$/,/^PENDING$/,/^SUSPENDED$/
            record[:state]="QUEUED"
          when /^RUNNING$/,/^COMPLETING$/
            record[:state]="RUNNING"
          when /^CANCELLED$/,/^FAILED$/,/^NODE_FAIL$/,/^PREEMPTED$/,/^TIMEOUT$/
            record[:state]="FAILED"
            record[:exit_status]=255 if record[:exit_status]==0 # Override exit status of 0 for "failed" jobs
          when /^COMPLETED$/
            if record[:exit_status]==0
              record[:state]="SUCCEEDED"
            else    
              record[:state]="FAILED"
            end
          else
            record[:state]="UNKNOWN"
        end
        record[:native_state]=jobfields["JobState"]

        # Add record to job queue
        @jobqueue[record[:jobid]]=record

      }  #  queued_jobs.find

    end  # job_queue

  end  # class

end  # module

