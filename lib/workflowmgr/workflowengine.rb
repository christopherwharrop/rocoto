##########################################
#
# Module WorkflowMgr
#
##########################################
module WorkflowMgr


  ##########################################
  #
  # Class WorkflowEngine
  #
  ##########################################
  class WorkflowEngine


    require 'drb'
    require 'workflowmgr/workflowconfig'
    require 'workflowmgr/workflowoption'
    require 'workflowmgr/launchserver'
    require 'workflowmgr/workflowlog'
    require 'workflowmgr/workflowdoc'
    require 'workflowmgr/workflowdb'
    require 'workflowmgr/cycledef'
    require 'workflowmgr/dependency'
    require 'workflowmgr/proxybatchsystem'

    ##########################################
    #
    # initialize
    #
    ##########################################
    def initialize(args)

      # Get command line options
      @options=WorkflowOption.new(args)

      # Get configuration file options
      @config=WorkflowYAMLConfig.new

      # Get the base directory of the WFM installation
      @wfmdir=File.dirname(File.dirname(File.expand_path(File.dirname(__FILE__))))

      # Set up an object to serve the workflow database (but do not open the database)
      setup_db_server

    end  # initialize


    ##########################################
    #
    # run
    #
    ##########################################
    def run

      begin

        # Open/Create the database
        @dbServer.dbopen

        # Acquire a lock on the workflow in the database
        @dbServer.lock_workflow

        # Set up an object to serve file stat info
        setup_filestat_server

        # Build the workflow objects from the contents of the workflow document
        build_workflow

        # Get the active cycles, including any new ones that need to be activated now
        get_active_cycles

        # Get the active jobs, which may include jobs from cycles that have just expired
        # as well as jobs needed for evaluating inter cycle dependencies
        get_active_jobs

        # Update the status of all active jobs
        update_active_jobs

        # Expire active cycles that have exceeded the cycle life span
        expire_cycles

        # Submit new tasks where possible
        submit_new_jobs

      ensure

        # Make sure we release the workflow lock in the database and shutdown the dbserver
        unless @dbServer.nil?
          @dbServer.unlock_workflow
          @dbServer.stop! if @config.DatabaseServer
        end

        # Make sure to shut down the workflow file stat server
        unless @fileStatServer.nil?
          @fileStatServer.stop! if @config.FileStatServer
        end

        # Make sure to shut down the workflow log server
        unless @logServer.nil?
          @logServer.stop! if @config.LogServer
        end

        # Shut down the batch queue server if it is no longer needed
        unless @bqServer.nil?
          @bqServer.stop! if @config.BatchQueueServer && !@bqServer.running?
        end

      end
 
    end  # run

  private


    ##########################################
    #
    # setup_db_server
    #
    ##########################################
    def setup_db_server

      begin

        # Initialize the database but do not open it (call dbopen to open it)
        database=WorkflowMgr::const_get("Workflow#{@config.DatabaseType}DB").new(@options.database)

        if @config.DatabaseServer
	  @dbServer=WorkflowMgr.launchServer("#{@wfmdir}/sbin/workflowdbserver")
          @dbServer.setup(database)
	else
          @dbServer=database
        end

      rescue

        # Print out the exception message
        puts $!

        # Try to stop the dbserver if something went wrong
        if @config.DatabaseServer
          @dbServer.stop! unless @dbServer.nil?
        end

      end


    end

    ##########################################
    #
    # setup_filestat_server
    #
    ##########################################
    def setup_filestat_server

      begin

        # Set up an object to serve requests for file stat info
        if @config.FileStatServer
          @fileStatServer=WorkflowMgr.launchServer("#{@wfmdir}/sbin/workflowfilestatserver")
          @fileStatServer.setup(File)
        else
          @fileStatServer=File
        end

      rescue

        # Print out the exception message
        puts $!

        # Try to stop the file stat server if something went wrong
        if @config.FileStatServer
          @fileStatServer.stop! unless @fileStatServer.nil?
        end

      end

    end


    ##########################################
    #
    # setup_bq_server
    #
    ##########################################
    def setup_bq_server(batchSystem)

      begin

        # Set up an object to serve requests for batch queue system services
        batchsystem=ProxyBatchSystem.new(batchSystem)

        if @config.BatchQueueServer
          @bqServer=WorkflowMgr.launchServer("#{@wfmdir}/sbin/workflowbqserver")
          @bqServer.setup(batchsystem)
        else
          @bqServer=batchsystem
        end

      rescue

        # Print out the exception message
        puts $!

        # Try to stop the batch queue server if something went wrong
        if @config.BatchQueueServer
          @bqServer.stop! unless @bqServer.nil?
        end

      end    

    end


    ##########################################
    #
    # setup_log_server
    #
    ##########################################
    def setup_log_server(log)

      begin

        # Set up an object to serve requests for batch queue system services
        if @config.LogServer
          @logServer=WorkflowMgr.launchServer("#{@wfmdir}/sbin/workflowlogserver")
          @logServer.setup(log)
        else
          @logServer=log
        end

      rescue

        # Print out the exception message
        puts $!

        # Try to stop the log server if something went wrong
        if @config.LogServer
          @logServer.stop! unless @logServer.nil?
        end

      end    

    end


    ##########################################
    #
    # build_workflow
    #
    ##########################################
    def build_workflow

      # Open the workflow document, parse it, and validate it
      if @fileStatServer.exists?(@options.workflowdoc)
        workflowdoc=WorkflowMgr::const_get("Workflow#{@config.WorkflowDocType}Doc").new(@options.workflowdoc)
      else
        puts $!
        raise "ERROR: Could not read workflow document '#{@options.workflowdoc}'"
      end

      # Get the realtime flag
      @realtime=workflowdoc.realtime?

      # Get the cycle life span
      @cyclelifespan=workflowdoc.cyclelifespan

      # Get the cyclethrottle
      @cyclethrottle=workflowdoc.cyclethrottle

      # Get the corethrottle
      @corethrottle=workflowdoc.corethrottle

      # Get the taskthrottle
      @taskthrottle=workflowdoc.taskthrottle

      # Get the scheduler
      setup_bq_server(workflowdoc.scheduler)

      # Get the log parameters
      setup_log_server(workflowdoc.log)

      # Get the cycle defs
      @cycledefs=workflowdoc.cycledefs

      # Get the tasks 
      @tasks=workflowdoc.tasks

      # Get the taskdep cycle offsets
      @taskdep_cycle_offsets=workflowdoc.taskdep_cycle_offsets

    end


    ##########################################
    #
    # build_dependency
    #
    ##########################################
    def build_dependency(node)

      # Build a dependency tree
      node.each { |nodekey,nodeval|
        case nodekey
          when :not
            return Dependency_NOT_Operator.new(nodeval.collect { |operand| build_dependency(operand) } )
          when :and
            return Dependency_AND_Operator.new(nodeval.collect { |operand| build_dependency(operand) } )
          when :or
            return Dependency_OR_Operator.new(nodeval.collect { |operand| build_dependency(operand) } )
          when :nand
            return Dependency_NAND_Operator.new(nodeval.collect { |operand| build_dependency(operand) } )
          when :nor
            return Dependency_NOR_Operator.new(nodeval.collect { |operand| build_dependency(operand) } )
          when :xor
            return Dependency_XOR_Operator.new(nodeval.collect { |operand| build_dependency(operand) } )
          when :some
            return Dependency_SOME_Operator.new(nodeval.collect { |operand| build_dependency(operand) }, node[:threshold] )
          when :datadep
            age=WorkflowMgr.ddhhmmss_to_seconds(node[:age])
            return DataDependency.new(CompoundTimeString.new(nodeval),age,@fileStatServer)
          when :taskdep
            task=@tasks.find {|t| t[:id]==nodeval }
            status=node[:status]
            cycle_offset=WorkflowMgr.ddhhmmss_to_seconds(node[:cycle_offset])
            
            return TaskDependency.new(task,status,cycle_offset)
          when :timedep
            return TimeDependency.new(CompoundTimeString.new(nodeval))
        end
      }

    end


    ##########################################
    #
    # get_active_cycles
    #
    ##########################################
    def get_active_cycles

      # Get active cycles from the database
      @active_cycles=@dbServer.get_active_cycles(@cyclelifespan)

      # Activate new cycles
      if @realtime
        newcycles=get_new_realtime_cycle
      else
        newcycles=get_new_retro_cycles
      end

      # Add new cycles to active cycle list and database
      unless newcycles.empty?

        # Add the new cycles to the database
        @dbServer.add_cycles(newcycles)

        # Add the new cycles to the list of active cycles
        @active_cycles += newcycles.collect { |cycle| {:cycle=>cycle, :activated=>Time.now.getgm} }

      end

    end


    ##########################################
    #
    # get_new_realtime_cycle
    #
    ##########################################
    def get_new_realtime_cycle

      # For realtime workflows, find the most recent cycle less than or equal to 
      # the current time and activate it if it has not already been activated

      # Get the most recent cycle <= now from cycle specs
      now=Time.now.getgm
      new_cycle=@cycledefs.collect { |c| c.previous(now) }.max

      # Get the latest cycle from the database or initialize it to a very long time ago
      latest_cycle=@dbServer.get_last_cycle || { :cycle=>Time.gm(1900,1,1,0,0,0) }

      # Return the new cycle if it hasn't already been activated
      if new_cycle > latest_cycle[:cycle]
        return [new_cycle]
      else
        return []
      end

    end

    ##########################################
    #
    # get_new_retro_cycles
    #
    ##########################################
    def get_new_retro_cycles

      # For retrospective workflows, find the next N cycles in chronological
      # order that have never been activated.  If any cycledefs have changed,
      # cycles may be returned that are older than previously activated cycles.
      # N is the cyclethrottle minus the number of currently active cycles.

      # Get the cycledefs from the database so that we can get their last known positions
      dbcycledefs=@dbServer.get_cycledefs.collect do |dbcycledef|
        case dbcycledef[:cycledef].split.size
          when 6
            CycleCron.new(dbcycledef[:cycledef],dbcycledef[:group],dbcycledef[:position])
          when 3
            CycleInterval.new(dbcycledef[:cycledef],dbcycledef[:group],dbcycledef[:position])
        end
      end
        
      # Update the positions of the current cycledefs (loaded from the workflowdoc) 
      # with their last known positions that are stored in the database
      @cycledefs.each do |cycledef|
        dbcycledef=dbcycledefs.find { |dbcycledef| dbcycledef.group==cycledef.group && dbcycledef.cycledef==cycledef.cycledef }
        next if dbcycledef.nil?
        cycledef.seek(dbcycledef.position)
      end

      # Get the set of cycles that are >= the earliest cycledef position
      cycleset=@dbServer.get_cycles(@cycledefs.collect { |cycledef| cycledef.position }.compact.min)

      # Sort the cycleset
      cycleset.sort { |a,b| a[:cycle] <=> b[:cycle] }

      # Find N new cycles to be added
      newcycles=[]
      (@cyclethrottle - @active_cycles.size).times do

        # Initialize the pool of new cycle candidates
        cyclepool=[]

        # Get the next new cycle for each cycle spec, and add it to the cycle pool
        @cycledefs.each do |cycledef|

          # Start looking for new cycles at the last known position for the cycledef
          next_cycle=cycledef.position

          # Iterate through cycles until we find a cycle that has not ever been activated
          # or we have tried all the cycles represented by the cycledef
          while !next_cycle.nil? do
            match=cycleset.find { |c| c[:cycle]==next_cycle }
            break if match.nil?
            next_cycle=cycledef.next(next_cycle + 60)
          end

          # If we found a new cycle, add it to the new cycle pool
          cyclepool << next_cycle unless next_cycle.nil?

          # Update cycledef position
          cycledef.seek(next_cycle)

        end  # cycledefs.each

        if cyclepool.empty?

          # If we didn't find any cycles that could be added, stop looking for more
          break

        else

          # The new cycle is the earlies cycle in the cycle pool
          newcycle=cyclepool.min

          # Add the earliest cycle in the cycle pool to the list of cycles to activate
          newcycles << newcycle

          # Add the new cycle to the cycleset so that we don't try to add it again
          cycleset << { :cycle=>newcycle }

        end  # if cyclepool.empty?

      end  # .times do

      # Save the workflowdoc cycledefs with their updated positions to the database
      @dbServer.set_cycledefs(@cycledefs.collect { |cycledef| { :group=>cycledef.group, :cycledef=>cycledef.cycledef, :position=>cycledef.position } } )

      return newcycles

    end  # activate_new_cycles


    ##########################################
    #
    # get_active_jobs
    #
    ##########################################
    def get_active_jobs

      # Initialize a list of job cycles corresponding to the set of active jobs
      job_cycles=[]

      # Get jobs for each active cycle and all cycle offsets from them
      @active_cycles.each do |active_cycle|

        # Add each active cycle to the active job cycles
        job_cycles << active_cycle[:cycle]

        # Add cycles for each known cycle offset in task dependencies
        # but only for cycles that have not just expired
        if Time.now - active_cycle[:activated] < @cyclelifespan
          @taskdep_cycle_offsets.each do |cycle_offset|
            job_cycles << active_cycle[:cycle] + cycle_offset
          end
        end

      end

      # Get all jobs whose cycle is in the job_cycle list
      @active_jobs=@dbServer.get_jobs(@active_cycles.collect { |c| c[:cycle] })

    end


    ##########################################
    #
    # update_active_jobs
    #
    ##########################################
    def update_active_jobs

      begin

        # Initialize hash of bqserver processes that are holding pending job ids
        bqservers={}

        # Initialize array of jobs whose job ids have been updated
        updated_jobs=[]  

        # Initialize counters for keeping track of active workflow parameters
        @active_task_count=0
        @active_core_count=0

        # Loop over all active jobs and retrieve and update their current status
        @active_jobs.keys.each do |taskname|
          @active_jobs[taskname].keys.each do |cycle|

            # No need to query or update the status of jobs that we already know are done
            next if @active_jobs[taskname][cycle][:state]=="done"

            # If the jobid is a DRb URI, retrieve the job submission status from the workflowbqserver process that submitted it
            if @active_jobs[taskname][cycle][:jobid]=~/^druby:/

              # Get the URI of the workflowbqserver that submitted the job
              uri=@active_jobs[taskname][cycle][:jobid]

              # Make a connection to the workflowbqserver at uri
              bqservers[uri]=DRbObject.new(nil, uri) unless bqservers.has_key?(uri)

              # Query the workflowbqserver for the status of the job submission 
              jobid,output=bqservers[uri].get_submit_status(taskname,cycle)

              # If there is no output from the submission, it means the submission is still pending
              if output.nil?
                @logServer.log(cycle,"Submission status of #{taskname} is still pending at #{uri}, something may be wrong.  Check to see if the #{uri} process is still alive")

              # Otherwise, the submission either succeeded or failed.
              else

                # If the job submission failed, log the output of the job submission command, and print it to stdout as well
                if jobid.nil?
                  # Delete the job from the database since it failed to submit.  It will be retried next time around.
                  @dbServer.delete_jobs([@active_jobs[taskname][cycle]])
                  puts output
                  @logServer.log(cycle,"Submission status of previously pending #{taskname} is failure!  #{output}")

                # If the job succeeded, record the jobid and log it
                else
                  @active_jobs[taskname][cycle][:jobid]=jobid
                  @logServer.log(cycle,"Submission status of previously pending #{taskname} is success, jobid=#{jobid}")
                end
              end
            end

            # Don't try to query the status of jobs whose submission is still pending
            next if @active_jobs[taskname][cycle][:jobid]=~/^druby:/

            # Get the status of the job from the batch system
            status=@bqServer.status(@active_jobs[taskname][cycle][:jobid])

            # Update the state of the job with its current state
            @active_jobs[taskname][cycle][:state]=status[:state]

            # Initialize a log message to report the job state
            logmsg="#{taskname} job id=#{@active_jobs[taskname][cycle][:jobid]} in state #{status[:state]}"

            # If the job has just finished, update exit status and tries and append info to log message
            if status[:state]=="done"
              @active_jobs[taskname][cycle][:exit_status]=status[:exit_status]
              @active_jobs[taskname][cycle][:tries]+=1
              logmsg+=", ran for #{status[:end_time] - status[:start_time]} seconds, exit status=#{status[:exit_status]}"

            # Otherwise increment counters
            else
              @active_task_count+=1
              @active_core_count+=@active_jobs[taskname][cycle][:cores]
            end

            # Log the state of the job
            @logServer.log(cycle,logmsg)

            # Add this job to the list of jobs whose state has been updated
            updated_jobs << @active_jobs[taskname][cycle]

          end # @active_jobs[taskname].keys.each
        end # @active_jobs.keys.each
     
      ensure

        # Make sure we save the updates to the database
        @dbServer.update_jobs(updated_jobs) unless updated_jobs.empty?

        # Make sure we always terminate all workflowbqservers that we no longer need
        unless bqservers.nil?
          bqservers.values.each { |bqserver| bqserver.stop! unless bqserver.running? }
        end

      end

    end


    ##########################################
    #
    # expire_cycles
    #
    ##########################################
    def expire_cycles

      active_cycles=[]
      expired_cycles=[]

      # Loop over all active cycles
      @active_cycles.each do |cycle|

        # If the cycle has expired, mark it, and delete jobs if necessary
        if Time.now.getgm - cycle[:activated].getgm > @cyclelifespan

          # Set the expiration time
          cycle[:expired]=Time.now.getgm

          # Add to list of expired cycles
          expired_cycles << cycle

        # Otherwise add the cycle to a new list of active cycles
        else
          active_cycles << cycle
        end

      end

      # Delete any jobs for the expired cycles
      expired_cycles.each do |cycle|          
        @active_jobs.keys.each do |taskname|
          next if @active_jobs[taskname][cycle[:cycle]].nil?
          unless @active_jobs[taskname][cycle[:cycle]][:state] == "done"
            @logServer.log(cycle[:cycle],"Deleting #{taskname} job #{@active_jobs[taskname][cycle[:cycle]][:jobid]} because this cycle has expired!")
            @bqServer.delete(@active_jobs[taskname][cycle[:cycle]][:jobid])
          end
        end
        @logServer.log(cycle[:cycle],"This cycle has expired!")
      end

      # Update the expired cycles in the database 
      @dbServer.update_cycles(expired_cycles)

      # Update the active cycle list
      @active_cycles=active_cycles

    end


    ##########################################
    #
    # submit_new_jobs
    #
    ##########################################
    def submit_new_jobs

      # Initialize an array of the new jobs that have been submitted
      newjobs=[]

      # Initialize a hash of task cycledefs
      taskcycledefs={}

      # Loop over active cycles and tasks, looking for eligible tasks to submit
      @active_cycles.collect { |c| c[:cycle] }.sort.each do |cycle|
        @tasks.each do |task|

          # Mqke sure the task is eligible for submission
          resubmit=false
          unless @active_jobs[task.attributes[:name]].nil?
            unless @active_jobs[task.attributes[:name]][cycle].nil?

              # Reject this task unless the existing job for it has completed
              next unless @active_jobs[task.attributes[:name]][cycle][:state] == "done"

              # Reject this task unless the existing job for it has crashed
              next unless @active_jobs[task.attributes[:name]][cycle][:exit_status] != 0

              # This task is a resubmission
              resubmit=true

            end
          end

          # Validate that this cycle is a member of at least one of the cycledefs specified for this task
          unless task.attributes[:cycledefs].nil?

            # Get the cycledefs associated with this task
            if taskcycledefs[task].nil?
              taskcycledefs[task]=@cycledefs.find_all { |cycledef| task.attributes[:cycledefs].split(/[\s,]+/).member?(cycledef.group) }
            end

            # Reject this task if the cycle is not a member of the tasks cycle list
            next unless taskcycledefs[task].any? { |cycledef| cycledef.member?(cycle) }

          end
          
          # Reject this task if dependencies are not satisfied
          next unless task.dependency.resolved?(cycle,@active_jobs,@fileStatServer)

          # Reject this task if retries has been exceeded
          if resubmit
            if @active_jobs[task.attributes[:name]][cycle][:tries] >= task.attributes[:maxtries]
              @logServer.log(cycle,"Cannot resubmit #{task.attributes[:name]}, maximum retry count of #{task.attributes[:maxtries]} has been reached")
              next
            end
          end

          # Reject this task if core throttle will be exceeded
          if @active_core_count + task.attributes[:cores] > @corethrottle
            @logServer.log(cycle,"Cannot submit #{task.attributes[:name]}, because maximum core throttle of #{@corethrottle} will be violated.")
            next
          end

          # Reject this task if task throttle will be exceeded
          if @active_task_count + 1 > @taskthrottle
            @logServer.log(cycle,"Cannot submit #{task.attributes[:name]}, because maximum task throttle of #{@taskthrottle} will be violated.")
            next
          end

          # Submit the task
          @bqServer.submit(task.localize(cycle),cycle)

          # Increment counters
          @active_core_count += task.attributes[:cores]
          @active_task_count += 1

          # If we are resubmitting the job, initialize the new job to the old job
          if resubmit
            newjob=@active_jobs[task.attributes[:name]][cycle]
          else
            newjob={:taskname=>task.attributes[:name], :cycle=>cycle, :tries=>0}
          end
          newjob[:state]="Submitting"
          newjob[:exit_status]=0
          newjob[:cores]=task.attributes[:cores]
          newjob[:jobid]=@bqServer.__drburi if @config.BatchQueueServer

          # Append the new job to the list of new jobs that were submitted
          newjobs << newjob

        end
      end        

      # If we are not using a batch queue server, make sure all qsub threads are terminated before checking for job ids
      Thread.list.each { |t| t.join unless t==Thread.main } unless @config.BatchQueueServer

      # Harvest job ids for submitted tasks
      newjobs.each do |job|
        uri=job[:jobid]
        jobid,output=@bqServer.get_submit_status(job[:taskname],job[:cycle])
        if output.nil?
          @logServer.log(job[:cycle],"Submitted #{job[:taskname]}.  Submission status is pending at #{job[:jobid]}")
        else
          if jobid.nil?
            puts output
            @logServer.log(job[:cycle],"Submission of #{job[:taskname]} failed!  #{output}")
          else
            job[:jobid]=jobid
            @logServer.log(job[:cycle],"Submitted #{job[:taskname]}, jobid=#{job[:jobid]}")
          end
        end
      end

      # Add the new jobs to the database
      @dbServer.add_jobs(newjobs)

    end


    ##########################################
    #
    # localize_task
    #
    ##########################################
    def localize_task(t,cycle)

      # Walk the task and evaluate all CompoundTimeStrings using the input cycle time
      if t.is_a?(Hash)
        lt={}
        t.each do |key,value|
          if key.is_a?(CompoundTimeString)
            lkey=key.to_s(cycle)
          else
            lkey=key
          end
          if value.is_a?(CompoundTimeString)
            lvalue=value.to_s(cycle)
          elsif value.is_a?(Hash) || value.is_a?(Array)
            lvalue=localize_task(value,cycle)
          else
            lvalue=value
          end
          lt[lkey]=lvalue        
        end
      elsif t.is_a?(Array)
        lt=t.collect do |value|
          if value.is_a?(CompoundTimeString)
            value.to_s(cycle)
          elsif value.is_a?(Hash) || value.is_a?(Array)
            localize_task(value,cycle)
          else
            value
          end
        end
      end 

      return lt

    end

  end  # Class WorkflowEngine

end  # Module WorkflowMgr
