###########################################
#
# Module WorkflowMgr
#
##########################################
module WorkflowMgr

  require 'workflowmgr/batchsystem'

  ##########################################
  #
  # Class SLURMBatchSystem
  #
  ##########################################
  class SLURMBatchSystem < BatchSystem

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

      # Initialize an empty hash for job accounting records
      @jobacct={}

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
      input="#! /bin/sh\n"

      # Add Slurm batch system options translated from the generic options specification
      task.attributes.each do |option,value|
        case option
          when :account
            input += "#SBATCH --account #{value}\n"
          when :queue            
            input += "#SBATCH -p #{value}\n"
          when :cores
            # Ignore this attribute if the "nodes" attribute is present
            next unless task.attributes[:nodes].nil?
            input += "#SBATCH --ntasks=#{value}\n"
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
            input += "#SBATCH --nodes=#{nnodes}-#{nnodes}\n"

            # Request max tasks per node
            input += "#SBATCH --tasks-per-node=#{maxppn}\n"

            # Make sure exclusive access to nodes is enforced
            input += "#SBATCH --exclusive\n"

            # Print a warning if multiple nodespecs are specified
            if nodespecs.size > 1
              WorkflowMgr.stderr("WARNING: SLURM does not support multiple types of node requests for batch jobs",1)
              WorkflowMgr.stderr("WARNING: You must use the -m option of the srun command in your script to launch your code with an arbitrary distribution of tasks",1)
              WorkflowMgr.stderr("WARNING: Please see https://computing.llnl.gov/linux/slurm/faq.html#arbitrary for details",1)
              WorkflowMgr.stderr("WARNING: Rocoto has automatically converted '#{value}' to '#{nnodes}:ppn=#{maxppn}' to facilitate the desired arbitrary task distribution",1)
              WorkflowMgr.stderr("WARNING: Use <nodes>#{nnodes}:ppn=#{maxppn}</nodes> in your workflow to eliminate this warning message.",1)
            end

           when :walltime
            # Make sure format is dd-hh:mm:ss if days are included
            input += "#SBATCH -t #{value.sub(/^(\d+):(\d+:\d+:\d+)$/,'\1-\2')}\n"
          when :memory
            m=/^([\.\d]+)([\D]*)$/.match(value)
            amount=m[1].to_f
            units=m[2]
            case units
              when /^B|b/
                amount=(amount / 1024.0 / 1024.0).ceil
              when /^K|k/
              amount=(amount / 1024.0).ceil
              when /^M|m/
                amount=amount.ceil
              when /^G|g/
                amount=(amount * 1024.0).ceil
              when nil
              amount=(amount / 1024.0 / 1024.0).ceil
            end
            if amount > 0
              input += "#SBATCH --mem=#{amount}\n"
            end
          when :stdout
            input += "#SBATCH -o #{value}\n"
          when :stderr
            input += "#SBATCH -e #{value}\n"
          when :join
            input += "#SBATCH -o #{value}\n"           
          when :jobname
            input += "#SBATCH --job-name #{value}\n"
        end
      end

      task.each_native do |value|
        input += "#SBATCH #{value}\n"
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
      tf=Tempfile.new('sbatch.in')
      tf.write(input)
      tf.flush()

      WorkflowMgr.stderr("Submitting #{task.attributes[:name]} using #{cmd} < #{tf.path} with input {{#{input}}}",4)

      # Run the submit command
      output=`#{cmd} < #{tf.path} 2>&1`.chomp()

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


    #####################################################
    #
    # refresh_jobacct
    #
    #####################################################
    def refresh_jobacct

      begin

        # Get the username of this process
        username=Etc.getpwuid(Process.uid).name

        # Run qstat to obtain the current status of queued jobs
        completed_jobs=""
        errors=""
        exit_status=0
        completed_jobs,errors,exit_status=WorkflowMgr.run4("sacct -o jobid,user%30,jobname%30,partition%20,priority,submit,start,end,ncpus,exitcode,state%12 -P",30)

        return if errors=~/SLURM accounting storage is disabled/

        # Raise SchedulerDown if the command failed
        raise WorkflowMgr::SchedulerDown,errors unless exit_status==0

        # Return if the output is empty
        return if completed_jobs.empty?

      rescue Timeout::Error,WorkflowMgr::SchedulerDown
        WorkflowMgr.log("#{$!}")
        WorkflowMgr.stderr("#{$!}",3)
        raise WorkflowMgr::SchedulerDown
      end

      # For each job, find the various attributes and create a job record
      completed_jobs.split("\n").each { |job|

        # Initialize an empty job record
  	record={}

  	# Look at all the attributes for this job and build the record
	jobfields=job.split("|")

        # Skip records for other users
        next unless jobfields[1] =~/^\s*#{username}$/

        # Extract job id        
        record[:jobid]=jobfields[0]

        # Extract job name
        record[:jobname]=jobfields[2]

        # Extract job owner
        record[:user]=jobfields[1].split("(").first

        # Extract core count
        record[:cores]=jobfields[8].to_i

        # Extract the partition
        record[:queue]=jobfields[3]

        # Extract the submit time
        record[:submit_time]=Time.local(*jobfields[5].split(/[-:T]/)).getgm

        # Extract the start time
        record[:start_time]=Time.local(*jobfields[6].split(/[-:T]/)).getgm

        # Extract the end time
        record[:end_time]=Time.local(*jobfields[7].split(/[-:T]/)).getgm

        # Extract the priority
        record[:priority]=jobfields[4]

        # Extract the exit status
        code,signal=jobfields[9].split(":").collect {|i| i.to_i}
        if code==0
          record[:exit_status]=signal
        else
          record[:exit_status]=code
        end            

        # Extract job state
        case jobfields[10]       
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
        record[:native_state]=jobfields[10]

        # Add record to job queue
        @jobacct[record[:jobid]]=record

      }  #  completed_jobs.find

    end  # job_acct

  end  # class

end  # module

