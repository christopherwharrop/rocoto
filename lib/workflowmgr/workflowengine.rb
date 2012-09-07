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
    require 'workflowmgr/workflowdoc'
    require 'workflowmgr/dbproxy'
    require 'workflowmgr/workflowioproxy'
    require 'workflowmgr/cycledef'
    require 'workflowmgr/dependency'
    require 'workflowmgr/bqsproxy'

    ##########################################
    #
    # initialize
    #
    ##########################################
    def initialize(options)

      begin

        # Get command line options
        @options=options

        # Get configuration file options
        @config=WorkflowYAMLConfig.new

        # Get the base directory of the WFM installation
        @wfmdir=File.dirname(File.dirname(File.expand_path(File.dirname(__FILE__))))

        # Set up an object to serve the workflow database (but do not open the database)
        @dbServer=DBProxy.new(@options.database,@config)

        # Initialize the workflow lock
        @locked=false

      rescue
        puts $!
        Process.exit(1)
      end

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
        @locked=@dbServer.lock_workflow
        Process.exit(0) unless @locked

        # Set up an object to serve file stat info
        @workflowIOServer=WorkflowIOProxy.new(@dbServer,@config)        

        # Build the workflow objects from the contents of the workflow document
        build_workflow

        # Get the active cycles
        get_active_cycles

        # Get new cycles that need to be activated now
        get_new_cycles

        # Get the active jobs, which may include jobs from cycles that have just expired
        # as well as jobs needed for evaluating inter cycle dependencies
        get_active_jobs

        # Update the status of all active jobs
        update_active_jobs

        # Deactivate completed cycles
        deactivate_done_cycles

        # Expire active cycles that have exceeded the cycle life span
        expire_cycles

        # Submit new tasks where possible
        submit_new_jobs

      rescue
        puts $!
        Process.exit(1)
        
      ensure

        # Shut down the batch queue server if it is no longer needed
        unless @bqServer.nil? || !@config.BatchQueueServer
          unless @bqServer.running?
            uri=@bqServer.__drburi
            @bqServer.stop!
            @dbServer.delete_bqservers([uri])
          end
        end

        # Make sure we release the workflow lock in the database and shutdown the dbserver
        unless @dbServer.nil?
          @dbServer.unlock_workflow if @locked
          @dbServer.stop! if @config.DatabaseServer
        end

        # Make sure to shut down the workflow file stat server
        unless @workflowIOServer.nil?
          @workflowIOServer.stop! if @config.WorkflowIOServer
        end

      end
 
    end  # run

    ##########################################
    #
    # boot
    #
    ##########################################
    def boot

      begin

        # Open/Create the database
        @dbServer.dbopen

        # Acquire a lock on the workflow in the database
        @locked=@dbServer.lock_workflow
        Process.exit(0) unless @locked

        # Get task name and cycle time
        boot_task_name=@options.tasks.first
        boot_cycle_time=@options.cycles.first

        # Set up an object to serve file stat info
        @workflowIOServer=WorkflowIOProxy.new(@dbServer,@config)        

        # Build the workflow objects from the contents of the workflow document
        build_workflow

        # Get the task from the workflow definition
        task=@tasks[boot_task_name]

        # Look for the cycle in the database to see if it has ever been activated
	boot_cycle=@dbServer.get_cycle(boot_cycle_time).first

        # Activate a new cycle if necessary and add it to the database
        if boot_cycle.nil?
          boot_cycle=Cycle.new(boot_cycle_time)
          boot_cycle.activate!
          @dbServer.add_cycles([boot_cycle])
          boot_job=nil
        else
          if boot_cycle.active?
            jobhash=@dbServer.get_jobs([boot_cycle_time])
            if jobhash[boot_task_name].nil?
              boot_job=nil
            else
              boot_job=jobhash[boot_task_name][boot_cycle_time]
            end
          else
            raise "Can not boot task '#{boot_task_name}' for cycle '#{boot_cycle_time.strftime("%Y%m%d%H%M")}' because the cycle is #{boot_cycle.state}"
          end
        end

        # Check for existing jobs that are not done
        unless boot_job.nil?
          if !boot_job.done?
            raise "Can not boot task '#{boot_task_name}' for cycle '#{boot_cycle_time.strftime("%Y%m%d%H%M")}' because there is already a job for it in state #{boot_job.state}"
          end
        end

        # Initialize jobid of the new job
        if @config.BatchQueueServer
          newjobid=@bqServer.__drburi
        else
          newjobid=0
        end

        # Create the job
        job = Job.new(newjobid,                            # jobid
                      boot_task_name,                      # taskname
                      boot_cycle_time,                     # cycle
                      task.attributes[:cores],             # cores
                      "SUBMITTING",                        # state
                      "SUBMITTING",                        # native state
                      0,                                   # exit_status
                      boot_job.nil? ? 0 : boot_job.tries+1,  # tries
                      0                                    # nunknowns
                     )

        # Add the new job to the database
	@dbServer.add_jobs([job])

        # Submit the task
	@bqServer.submit(task.localize(boot_cycle_time),boot_cycle_time)
        @logServer.log(boot_cycle_time,"Forcibly submitting #{task.attributes[:name]}")

        # If we are not using a batch queue server, make sure all qsub threads are terminated before checking for job ids
        Thread.list.each { |t| t.join unless t==Thread.main } unless @config.BatchQueueServer

        # Harvest job ids for submitted tasks
        uri=job.id
        jobid,output=@bqServer.get_submit_status(job.task,job.cycle)
        if output.nil?
          @logServer.log(job.cycle,"Submission status of #{job.task} is pending at #{job.id}")
        else
          if jobid.nil?
            # Delete the job from the database since it failed to submit.  It will be retried next time around.
            @dbServer.delete_jobs([job])
            puts output
            @logServer.log(job.cycle,"Submission of #{job.task} failed!  #{output}")
          else
            job.id=jobid
            job.state="QUEUED"
            job.native_state="queued"
            @logServer.log(job.cycle,"Submission of #{job.task} succeeded, jobid=#{job.id}")
            # Update the jobid for the job in the database
            @dbServer.update_jobs([job])
          end
        end

      rescue
        puts $!
        Process.exit(1)
        
      ensure

        # Shut down the batch queue server if it is no longer needed
        unless @bqServer.nil? || !@config.BatchQueueServer
          unless @bqServer.running?
            uri=@bqServer.__drburi
            @bqServer.stop!
            @dbServer.delete_bqservers([uri])
          end
        end

        # Make sure we release the workflow lock in the database and shutdown the dbserver
        unless @dbServer.nil?
          @dbServer.unlock_workflow if @locked
          @dbServer.stop! if @config.DatabaseServer
        end

        # Make sure to shut down the workflow file stat server
        unless @workflowIOServer.nil?
          @workflowIOServer.stop! if @config.WorkflowIOServer
        end

      end

    end

  private

    ##########################################
    #
    # build_workflow
    #
    ##########################################
    def build_workflow

      # Open the workflow document, parse it, and validate it
      workflowdoc=WorkflowMgr::const_get("Workflow#{@config.WorkflowDocType}Doc").new(@options.workflowdoc,@workflowIOServer)

      # Get the realtime flag
      @realtime=workflowdoc.realtime?

      # Get the cycle life span
      @cyclelifespan=workflowdoc.cyclelifespan || WorkflowMgr.ddhhmmss_to_seconds("365:00:00:00")

      # Get the cyclethrottle
      @cyclethrottle=workflowdoc.cyclethrottle || 1

      # Get the corethrottle
      @corethrottle=workflowdoc.corethrottle || 9999999

      # Get the taskthrottle
      @taskthrottle=workflowdoc.taskthrottle || 9999999

      # Get the scheduler
      @bqServer=BQSProxy.new(workflowdoc.scheduler,@options.database,@config)
      
      # Add this scheduler to the bqserver database if needed
      @dbServer.add_bqservers([@bqServer.__drburi]) if @config.BatchQueueServer

      # Get the log parameters
      @logServer=workflowdoc.log

      # Get the cycle defs
      @cycledefs=workflowdoc.cycledefs

      # Get the tasks 
      @tasks=workflowdoc.tasks

      # Get the taskdep cycle offsets
      @taskdep_cycle_offsets=workflowdoc.taskdep_cycle_offsets

    end


    ##########################################
    #
    # get_active_cycles
    #
    ##########################################
    def get_active_cycles

      # Get active cycles from the database
      @active_cycles=@dbServer.get_active_cycles

    end


    ##########################################
    #
    # get_new_cycles
    #
    ##########################################
    def get_new_cycles

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
        @active_cycles += newcycles

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

      # Get the most recent cycle time <= now from cycle specs
      now=Time.now.getgm
      latest_cycle_time=@cycledefs.collect { |c| c.previous(now) }.max
      latest_cycle=Cycle.new(latest_cycle_time, { :activated=>latest_cycle_time.getgm } )

      # Get the latest cycle from the database or initialize it to a very long time ago
      latest_db_cycle=@dbServer.get_last_cycle || Cycle.new(Time.gm(1900,1,1,0,0,0))

      # Return the new cycle if it hasn't already been activated
      if latest_cycle > latest_db_cycle
        return [latest_cycle]
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
      cycleset=@dbServer.get_cycles( { :start=>@cycledefs.collect { |cycledef| cycledef.position }.compact.min } )

      # Sort the cycleset
      cycleset.sort!

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
            match=cycleset.find { |c| c.cycle==next_cycle }
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

          # The new cycle is the earliest cycle in the cycle pool
          newcycle=Cycle.new(cyclepool.min, { :activated=>Time.now.getgm } )

          # Add the earliest cycle in the cycle pool to the list of cycles to activate
          newcycles << newcycle

          # Add the new cycle to the cycleset so that we don't try to add it again
          cycleset << newcycle

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
        job_cycles << active_cycle.cycle

        # Add cycles for each known cycle offset in task dependencies
        # but only for cycles that have not just expired
        if Time.now - active_cycle.activated < @cyclelifespan
          @taskdep_cycle_offsets.each do |cycle_offset|
            job_cycles << active_cycle.cycle + cycle_offset
          end
        end

      end

      # Get all jobs whose cycle is in the job_cycle list
      @active_jobs=@dbServer.get_jobs(job_cycles)

    end


    ##########################################
    #
    # harvest_pending_jobids
    #
    ##########################################
    def harvest_pending_jobids

      # Initialize hash of old bqserver processes from the database and establish connections to them
      bqservers={}
      @dbServer.get_bqservers.each do |uri|

        begin

          # We are only interested in old bqserver processes
          next if uri==@bqServer.__drburi

          bqservers[uri]=DRbObject.new(nil, uri) unless bqservers.has_key?(uri)

        # The bqserver has died!
        rescue DRb::DRbConnError
          # Remove the bqserver uri from the database
          @dbServer.delete_bqservers([uri])

          # Remove the bqserver uri from the bqservers list if needed
          bqservers.delete(uri) if bqservers.has_key?(uri)
        end

      end

      begin

        # Loop over active jobs looking for ones with pending submissions
        @active_jobs.values.collect { |cyclehash| cyclehash.values }.flatten.sort_by { |job| [job.cycle, @tasks[job.task].seq] }.each do |job|

          # Skip jobs that don't have pending job ids
          next unless job.pending_submit?

          # Get the URI of the workflowbqserver that submitted the job
          uri=job.id

          begin

            # Query the workflowbqserver for the status of the job submission 
            jobid,output=bqservers[uri].get_submit_status(job.task,job.cycle) if bqservers.has_key?(uri)

          # Catch exceptions for bqservers that have died unexpectedly
          rescue DRb::DRbConnError

            # Remove the bqserver uri from the database
            @dbServer.delete_bqservers([uri])

            # Remove the bqserver uri from the bqservers list if needed
            bqservers.delete(uri) if bqservers.has_key?(uri)

          end

          # If the bqserver died, warn user, resubmit job
          if !bqservers.has_key?(uri)

            # Log the fact that the submission status could not be retrieved
            puts "Submission status of #{job.task} for cycle #{job.cycle.strftime("%Y%m%d%H%M")} could not be retrieved because the server process at #{uri} died"
            puts "Submission of #{job.task} for cycle #{job.cycle.strftime("%Y%m%d%H%M")} probably, but not necessarily, failed.  It will be resubmitted"
            @logServer.log(job.cycle,"Submission status of #{job.task} for cycle #{job.cycle.strftime("%Y%m%d%H%M")} could not be retrieved because the server process at #{uri} died")
            @logServer.log(job.cycle,"Submission of #{job.task} for cycle #{job.cycle.strftime("%Y%m%d%H%M")} probably, but not necessarily, failed.  It will be resubmitted")

            # Delete the job from the database since it failed to submit.  It will be retried immediately.
            @dbServer.delete_jobs([job])

            # Remove the job from the active_jobs list since it failed to submit and is not active.
            @active_jobs.delete(job)

            next

          # If there is no output from the submission, it means the submission is still pending
          elsif output.nil?
            @logServer.log(job.cycle,"Submission status of #{job.task} is still pending at #{uri}.  The batch system server may be down, unresponsive, or under heavy load.")

          # Otherwise, the submission either succeeded or failed.
          else

            # If the job submission failed, log the output of the job submission command, and print it to stdout as well
            if jobid.nil?

              # Delete the job from the database since it failed to submit.  It will be retried immediately.
              @dbServer.delete_jobs([job])

              # Remove the job from the active_jobs list since it failed to submit and is not active.
              @active_jobs.delete(job)

              puts output
              @logServer.log(job.cycle,"Submission status of previously pending #{job.task} is failure!  #{output}")

              next

            # If the job succeeded, record the jobid and log it
            else
              job.id=jobid
              @logServer.log(job.cycle,"Submission status of previously pending #{job.task} is success, jobid=#{jobid}")
            end

          end  # if output.nil?

          # Update the job in the database
          @dbServer.update_jobs([job])

        end # each job

      ensure

        # Make sure we always terminate all workflowbqservers that we no longer need
        bqservers.each do |uri,bqserver|

          begin
            unless bqserver.running? 
              bqserver.stop!
              @dbServer.delete_bqservers([uri])
            end
          # Catch exceptions for bqservers that have died unexpectedly
          rescue DRb::DRbConnError
            puts "WARNING: BQS Server process at #{uri} died unexpectedly.  Submission status of some jobs may have been lost"
            @dbServer.delete_bqservers([uri])
          end

        end

      end  # begin

    end


    ##########################################
    #
    # update_active_jobs
    #
    ##########################################
    def update_active_jobs

      # Harvest job ids from pending job submissions
      harvest_pending_jobids

      begin

        # Initialize array of jobs whose job ids have been updated
        updated_jobs=[]  

        # Initialize counters for keeping track of active workflow parameters
        @active_task_count=0
        @active_core_count=0

        # Loop over all active jobs and retrieve and update their current status
        @active_jobs.values.collect { |cyclehash| cyclehash.values }.flatten.sort_by { |job| [job.cycle, @tasks[job.task].seq] }.each do |job|

          # No need to query or update the status of jobs that we already know are done successfully or that remain failed
          # If a job is failed at this point, it could only be because the WFM crashed before a resubmit or state update could occur
          next if job.state=="SUCCEEDED" || job.state=="FAILED"

          # Resurrect DEAD tasks if the user increased the task maxtries sufficiently to enable more attempts
          if job.state=="DEAD"
            if job.tries < @tasks[job.task].attributes[:maxtries]

              # Reset the state to FAILED so a resubmission can occur
              job.state="FAILED"

              # Update the state of the job in the database
              @dbServer.update_jobs([job])                

              # Log the fact that this job was resurrected
              @logServer.log(job.cycle,"Task #{job.task} has been resurrected.  #{@tasks[job.task].attributes[:maxtries] - job.tries} more tries will be allowed")
            end
 
            # No need for more updates to this job
            next

          end

          # Get the status of the job from the batch system
          status=@bqServer.status(job.id)

          # Update the state of the job with its current state
          job.state=status[:state]
          job.native_state=status[:native_state]            
          if status[:state]=="SUCCEEDED" || status[:state]=="FAILED"
            job.exit_status=status[:exit_status]
            if status[:start_time]==Time.at(0).getgm
              duration=0
            else
              duration=status[:end_time] - status[:start_time]
            end
            runmsg=", ran for #{duration} seconds, exit status=#{status[:exit_status]}"
          else
            runmsg=""
          end

          # Check for recurring state of UNKNOWN
          if job.state=="UNKNOWN"

            # Increment unknown counter
            job.nunknowns+=1

            # Assume the job failed if too many consecutive UNKNOWNS
            unknownmsg=""
            if job.nunknowns >= @config.MaxUnknowns
              job.state="FAILED"
              unknownmsg+=", giving up because job state could not be determined #{job.nunknowns} consecutive times"
            end

          else
            # Reset unknown counter to zero if not in UNKNOWN state
            job.nunknowns=0
            unknownmsg=""
          end

          # Check for maxtries violation and update counters
          if job.state=="SUCCEEDED" || job.state=="FAILED"
            job.tries+=1
            if job.state=="FAILED"
              if job.tries >= @tasks[job.task].attributes[:maxtries]
                job.state="DEAD"
              end
            end
            triesmsg=", try=#{job.tries} (of #{@tasks[job.task].attributes[:maxtries]})"
          else
            # Update counters for jobs that are still QUEUED, RUNNING, or UNKNOWN
            @active_task_count+=1
            @active_core_count+=job.cores
            triesmsg=""
          end

          statemsg="Task #{job.task}, jobid=#{job.id}, in state #{job.state} (#{job.native_state})"

          # Update the job state in the database
          @dbServer.update_jobs([job])

          # Log the state of the job
          @logServer.log(job.cycle,statemsg+runmsg+unknownmsg+triesmsg)

        end # @active_jobs.each
     
      end

    end


    ##########################################
    #
    #  deactivate_done_cycles
    #
    ##########################################
    def deactivate_done_cycles

      active_cycles=[]
      done_cycles=[]

      # Initialize a hash of task cycledefs
      taskcycledefs={}

      # Loop over all active cycles
      @active_cycles.each do |cycle|

        # Initialize done flag to true for this cycle
        cycle_done=false
        cycle_success=true
        
        catch (:not_done) do

          # Loop over all tasks
          @tasks.values.sort { |t1,t2| t1.seq <=> t2.seq }.each do |task|

            # Validate that this cycle is a member of at least one of the cycledefs specified for this task
            unless task.attributes[:cycledefs].nil?

              # Get the cycledefs associated with this task
              if taskcycledefs[task].nil?
                taskcycledefs[task]=@cycledefs.find_all { |cycledef| task.attributes[:cycledefs].split(/[\s,]+/).member?(cycledef.group) }
              end

              # Reject this task if the cycle is not a member of the tasks cycle list
              next unless taskcycledefs[task].any? { |cycledef| cycledef.member?(cycle.cycle) }

            end  # unless

            # The cycle is not done if this task has not been submitted yet for any of the active cycles
            throw :not_done if @active_jobs[task.attributes[:name]].nil?

            # The cycle is not done if this task has not been submitted yet for this cycle
            throw :not_done if @active_jobs[task.attributes[:name]][cycle.cycle].nil?

            # The cycle is not done if the job for this task and cycle is not in the done state
            throw :not_done if @active_jobs[task.attributes[:name]][cycle.cycle].state != "SUCCEEDED"

# For now, only tag cycles as done if they are done successfully, meaning that all tasks are complete and have exit status = 0.
# If we mark cycles as done when they have tasks that exceeded retries, then increasing retries won't cause them to rerun again
#
#            # The cycle is not done if the job for this task and cycle is done, but has crashed and has not yet exceeded the retry count
#            if @active_jobs[task.attributes[:name]][cycle.cycle].tries >= task.attributes[:maxtries]
#              cycle_success=false
#            else
#              throw :not_done if @active_jobs[task.attributes[:name]][cycle.cycle].exit_status != 0 
#            end

          end  # tasks.each

          cycle_done=true
          
        end  # catch

        # If the cycle is done, record the time and update active cycle list
        if cycle_done
        
          # Mark the cycle as done
          cycle.done!

          # Add to list of done cycles
          done_cycles << cycle

          # Log the done status of this cycle
          if cycle_success
            @logServer.log(cycle.cycle,"This cycle is complete: Success") 
          else
            @logServer.log(cycle.cycle,"This cycle is complete: Failed") 
          end

          # Update the done cycle in the database 
          @dbServer.update_cycles([cycle])

        # Otherwise add the cycle to a new list of active cycles
        else
          active_cycles << cycle
        end

      end  # active_cycles.each

      # Update the active cycle list
      @active_cycles=active_cycles

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
        if Time.now.getgm - cycle.activated.getgm > @cyclelifespan

          # Set the expiration time
          cycle.expire!

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
          next if @active_jobs[taskname][cycle.cycle].nil?
          unless @active_jobs[taskname][cycle.cycle].state == "SUCCEEDED" || @active_jobs[taskname][cycle.cycle].state == "FAILED" || @active_jobs[taskname][cycle.cycle].state == "DEAD"
            @logServer.log(cycle.cycle,"Deleting #{taskname} job #{@active_jobs[taskname][cycle.cycle].id} because this cycle has expired!")
            @bqServer.delete(@active_jobs[taskname][cycle.cycle].id)
          end
        end

        @logServer.log(cycle.cycle,"This cycle has expired!")
        
        # Update the expired cycles in the database
        @dbServer.update_cycles([cycle]) 
      end

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
      @active_cycles.collect { |c| c.cycle }.sort.each do |cycle|
        @tasks.values.sort { |t1,t2| t1.seq <=> t2.seq }.each do |task|

          # Mqke sure the task is eligible for submission
          resubmit=false
          unless @active_jobs[task.attributes[:name]].nil?
            unless @active_jobs[task.attributes[:name]][cycle].nil?

              # Since this task has already been submitted at least once, reject it unless the job for it has failed
              next unless @active_jobs[task.attributes[:name]][cycle].state == "FAILED"

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
          
          # Reject this task if core throttle will be exceeded
          if @active_core_count + task.attributes[:cores] > @corethrottle
            @logServer.log(cycle,"Cannot submit #{task.attributes[:name]}, because maximum core throttle of #{@corethrottle} will be violated.",2)
            next
          end

          # Reject this task if task throttle will be exceeded
          if @active_task_count + 1 > @taskthrottle
            @logServer.log(cycle,"Cannot submit #{task.attributes[:name]}, because maximum task throttle of #{@taskthrottle} will be violated.",2)
            next
          end

          # Reject this task if dependencies are not satisfied
          unless task.dependency.nil?
            next unless task.dependency.resolved?(cycle,@active_jobs,@workflowIOServer)
          end

          # Reject this task if retries has been exceeded
          # This code block should never execute since state should be DEAD if retries is exceeded and we should never get here for a DEAD job
          if resubmit
            if @active_jobs[task.attributes[:name]][cycle].tries >= task.attributes[:maxtries]
              @logServer.log(cycle,"Cannot resubmit #{task.attributes[:name]}, maximum retry count of #{task.attributes[:maxtries]} has been reached")
              next
            end
          end

          # Increment counters
          @active_core_count += task.attributes[:cores]
          @active_task_count += 1

          # If we are resubmitting the job, initialize the new job to the old job
          if @config.BatchQueueServer
            newjobid=@bqServer.__drburi 
          else
            newjobid=0
          end
          if resubmit
            newjob = Job.new(newjobid,                    # jobid
                             task.attributes[:name],   # taskname
                             cycle,                    # cycle
                             task.attributes[:cores],  # cores
                             "SUBMITTING",             # state
                             "SUBMITTING",             # native state
                             0,                        # exit_status
                             @active_jobs[task.attributes[:name]][cycle].tries,    # tries
                             0                         # nunknowns
                            )

          else
            newjob = Job.new(newjobid,                    # jobid
                             task.attributes[:name],   # taskname
                             cycle,                    # cycle
                             task.attributes[:cores],  # cores
                             "SUBMITTING",             # state
                             "SUBMITTING",             # native state
                             0,                        # exit_status
                             0,                        # tries
                             0                         # nunknowns
                            )          
          end
 

          # Append the new job to the list of new jobs that were submitted
          newjobs << newjob

          # Add the new job to the database
          @dbServer.add_jobs([newjob])

          # Submit the task
          @bqServer.submit(task.localize(cycle),cycle)
          @logServer.log(cycle,"Submitting #{task.attributes[:name]}")

        end

      end        

      # If we are not using a batch queue server, make sure all qsub threads are terminated before checking for job ids
      Thread.list.each { |t| t.join unless t==Thread.main } unless @config.BatchQueueServer

      # Harvest job ids for submitted tasks
      newjobs.each do |job|
        uri=job.id
        jobid,output=@bqServer.get_submit_status(job.task,job.cycle)
        if output.nil?
          @logServer.log(job.cycle,"Submission status of #{job.task} is pending at #{job.id}")
        else
          if jobid.nil?
            # Delete the job from the database since it failed to submit.  It will be retried next time around.
            @dbServer.delete_jobs([job])
            puts output
            @logServer.log(job.cycle,"Submission of #{job.task} failed!  #{output}")
          else
            job.id=jobid
            job.state="QUEUED"
            job.native_state="queued"
            @logServer.log(job.cycle,"Submission of #{job.task} succeeded, jobid=#{job.id}")
            # Update the jobid for the job in the database
            @dbServer.update_jobs([job])
          end
        end
      end

    end


  end  # Class WorkflowEngine

end  # Module WorkflowMgr
