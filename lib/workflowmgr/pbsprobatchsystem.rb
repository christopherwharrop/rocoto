###########################################
#
# Module WorkflowMgr
#
##########################################
module WorkflowMgr

  require 'workflowmgr/batchsystem'

  ##########################################
  #
  # Class PBSPROBatchSystem
  #
  ##########################################
  class PBSPROBatchSystem < BatchSystem

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
    def initialize(pbspro_root=nil,config)

      # Get timeouts from the configuration
      @qstat_x_timeout=config.JobAcctTimeout

      # Initialize an empty hash for job completion records
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

        # Initialize statuses to UNAVAILABLE
        jobStatuses={}
        jobids.each do |jobid|
          jobStatuses[jobid] = { :jobid => jobid, :state => "UNAVAILABLE", :native_state => "Unavailable" }
        end

        raise WorkflowMgr::SchedulerDown unless @schedup

        # Populate the job accounting log table if it is empty
        refresh_jobacct(jobids) if @jobacct.empty?

        # Collect the statuses of the jobs
        jobids.each do |jobid|
          if @jobacct.has_key?(jobid)
            jobStatuses[jobid] = @jobacct[jobid]
          else
            jobStatuses[jobid] = { :jobid => jobid, :state => "UNKNOWN", :native_state => "Unknown" }
          end
        end

      rescue WorkflowMgr::SchedulerDown
        @schedup=false
      ensure
        return jobStatuses
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

      # Add Pbspro batch system options translated from the generic options specification
      task.attributes.each do |option,value|
         if value.is_a?(String)
           if value.empty?
             WorkflowMgr.stderr("WARNING: <#{option}> has empty content and is ignored", 1)
             next
           end
        end
        case option
          when :account
            input += "#PBS -A #{value}\n"
          when :queue
            input += "#PBS -q #{value}\n"
          when :partition
            WorkflowMgr.stderr("WARNING: the <partition> tag is not supported for PBSPro.", 1)
            WorkflowMgr.log("WARNING: the <partition> tag is not supported for PBSPro.")
          when :nodesize
            WorkflowMgr.stderr("WARNING: <nodesize> support is deprecated, please use <nodes> to specify the requested resources", 1)
            WorkflowMgr.log("WARNING: <nodesize> support is deprecated, please use <nodes> to specify the requested resources")
          when :cores
            # Ignore this attribute if the "nodes" attribute is present
            next unless task.attributes[:nodes].nil?

            # Print deprecation warning
            WorkflowMgr.stderr("WARNING: <cores> support is deprecated for PBSPro, please use <nodes> to specify the requested resources", 1)
            WorkflowMgr.log("WARNING: <cores> support is deprecated for PBSPro, please use <nodes> to specify the requested resources")

            # Get the node size
            nodesize = task.attributes[:nodesize]
            if nodesize.nil?
              WorkflowMgr.stderr("FATAL ERROR: task `#{task.attributes[:name]}` cannot be submitted due to missing <nodesize> information",0)
              WorkflowMgr.log("FATAL ERROR: task `#{task.attributes[:name]}` cannot be submitted due to missing <nodesize> information")
              return nil, "FATAL ERROR: task `#{task.attributes[:name]}` cannot be submitted due to missing <nodesize> information"
            end

            # Calculate the number of full nodes required
            nchunks = value / nodesize

            if nchunks > 0

              # Set the selection for full nodes
              input += "#PBS -l select=#{nchunks}:ncpus=#{nodesize}:mpiprocs=#{nodesize}"

              # Add selection of memory if requested
              input += ":mem=#{task.attributes[:memory]}" unless task.attributes[:memory].nil?

            end

            # Add a chunk for non-full node if needed
            leftovers = value % nodesize

            if leftovers > 0

              if nchunks > 0
                input += "+"
              else
                input += "#PBS -l select="
              end

              input += "1:ncpus=#{leftovers}:mpiprocs=#{leftovers}"

              # Add selection of memory if requested
              input += ":mem=#{task.attributes[:memory]}" unless task.attributes[:memory].nil?

            end

            input += "\n"
          when :nodes
             # Set up -l select option
             input += "#PBS -l select="
             # Add spec for each select chunk
             value.strip.split("+").each { |chunk|
               mpiprocs=1
               ompthreads=1
               # ppn corresponds to mpiprocs
               if chunk=~/ppn=(\d+)/
                 mpiprocs=$1.to_i
               end
               # tpp corresponds to ompthreads per mpiproc
               if chunk=~/tpp=(\d+)/
                 ompthreads=$1.to_i
               end
               input += chunk.gsub(/ppn=\d+/,"mpiprocs=#{mpiprocs}").gsub(/tpp=\d+/,"ompthreads=#{ompthreads}")
               input += ":ncpus=#{mpiprocs * ompthreads}"
               input += ":mem=#{task.attributes[:memory]}" unless task.attributes[:memory].nil?
               input += "+"
             }
             input.sub!(/\+$/,"\n")
          when :walltime
            input += "#PBS -l walltime=#{value}\n"
          when :memory
            # This is handled by :nodes and :cores
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
    # refresh_jobacct
    #
    #####################################################
    def refresh_jobacct(jobids)

      begin

        # Get the username of this process
        username=Etc.getpwuid(Process.uid).name

        # Run qstat to obtain the current status of queued jobs
        joblist=""
        qstat=""
        errors=""
        exit_status=0

        unless jobids.nil?
          joblist = jobids.join(" ")
        end

        # Return if the joblist is empty
        return if joblist.empty?

        # Get the status of jobs in the job list
        qstat,errors,exit_status=WorkflowMgr.run4("qstat -x -f #{joblist} | sed -e ':a' -e 'N' -e '$\!ba' -e 's/\\n\\t/ /g'", @qstat_x_timeout)

        # Raise SchedulerDown if the qstat failed
        raise WorkflowMgr::SchedulerDown,errors unless exit_status==0

        # Return if the qstat output is empty
        return if qstat.empty?

      rescue Timeout::Error,WorkflowMgr::SchedulerDown
        WorkflowMgr.log("#{$!}")
        WorkflowMgr.stderr("#{$!}",3)
        raise WorkflowMgr::SchedulerDown
      end

      # Initialize an empty job record
      record={}

      # For each line, find the various attributes and create job records
      qstat.each_line { |line|

        # Remove leading and trailing white space
        line.strip!

        # State a new job record if this is a blank line
        if line.empty?

          # If the job is complete and has an exit status, change the state to SUCCEEDED or FAILED
          if record[:state]=="UNKNOWN" && !record[:exit_status].nil?
            if record[:exit_status]==0
              record[:state]="SUCCEEDED"
            else
              record[:state]="FAILED"
            end
          end

          @jobacct.merge!({ record[:jobid] => record }) if record[:user] =~ /^#{username}/

          record={}
          next
        elsif line =~ /^Job Id: (\d+)/
          record[:jobid] = $1
        elsif line =~ /^([^=]+) = (.*)$/
          key=$1
          value=$2
          case (key)
            when "Variable_List"
              next
            when "exec_host"
              next
            when "exec_vnode"
              next
            when "estimated.exec_vnode"
              next
            when "comment"
              next
            when "Job_Owner"
              record[:user] = value.split("@")[0]
            when "job_state"
              record[:native_state] = value
              case value
                when /^Q$/,/^H$/,/^W$/,/^S$/,/^T$/,/^M$/
                  record[:state] = "QUEUED"
                when /^B$/,/^R$/,/^E$/
                  record[:state]="RUNNING"
                else
                  record[:state]="UNKNOWN"
              end
            when "Exit_status"
              record[:exit_status] = value.to_i
            when /Job_Name/
              record[:jobname] = value
            when /queue/
              record[:queue]=value
            when /ctime/
              record[:submit_time]=Time.local(*ParseDate.parsedate(value)).getgm
            when /stime/
              record[:start_time]=Time.local(*ParseDate.parsedate(value)).getgm
            when /mtime/
              record[:end_time]=Time.local(*ParseDate.parsedate(value)).getgm
            when /resources_used.ncpus/
              record[:cores]=value
            when /Priority/
              record[:priority]=value.to_i

            else
#              record[key] = value
          end
        end

      }  #  qstat.each

    end  # refresh_job_queue

  end  # class

end  # module
