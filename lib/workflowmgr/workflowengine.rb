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
    require 'workflowmgr/sgebatchsystem'
    require 'workflowmgr/jobsubmitter'


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

    end  # initialize


    ##########################################
    #
    # run
    #
    ##########################################
    def run

      # Initialize the database
      @workflowdb=eval "Workflow#{@config.DatabaseType}DB.new(@options.database)"

      # Acquire a lock on the workflow in the database
      @workflowdb.lock_workflow

      begin

        # Initialize the workflow document
        @workflowdoc=eval "Workflow#{@config.WorkflowDocType}Doc.new(@options.workflowdoc)"

        # Build the workflow from the workflow document
        build_workflow

        # Get active cycles from the database
        @active_cycles=@workflowdb.get_active_cycles(@cyclelifespan)

        # Activate new cycles if possible
        activate_new_cycles

        # Get active jobs from the database
        @active_jobs=@workflowdb.get_jobs

        # Update the status of all active jobs
        updated_jobs=[]
        @active_jobs.each_key do |task|
          @active_jobs[task].each_key do |cycle|
            jobid=@active_jobs[task][cycle][:jobid]
            if jobid=~/^druby:/ 
              uri=jobid
              jobid,output=get_task_submit_status(jobid)
              if output.nil?
                @logserver.log(cycle[:cycle],"Submission status of #{task} is still pending at #{uri}, something may be wrong.  Check to see if the #{uri} process is still alive")
              else
                if jobid.nil?
                  puts output
                  @logserver.log(cycle,"Submission status of previously pending #{task} is failure!  #{output}")
                else
                  @active_jobs[task][cycle][:jobid]=jobid
                  @logserver.log(cycle,"Submission status of previously pending #{task} is success, jobid=#{jobid}")
                end
              end
            else
              unless @active_jobs[task][cycle][:state]=="done"
                status=@scheduler.status(@active_jobs[task][cycle][:jobid])
                @active_jobs[task][cycle][:state]=status[:state]
                if status[:state]=="done"
                  @logserver.log(cycle,"#{task} job id=#{jobid} in state #{status[:state]}, exit status=#{status[:exit_status]}")
                else
                  @logserver.log(cycle,"#{task} job id=#{jobid} in state #{status[:state]}")
                end
              end
            end         
            updated_jobs << @active_jobs[task][cycle]
          end
        end
        @workflowdb.update_jobs(updated_jobs)

        # Submit new tasks where possible
        newjobs=[]
        @active_cycles.each do |cycle|
          @tasks.each do |task|
            unless @active_jobs[task[:id]].nil?
              next unless @active_jobs[task[:id]][cycle[:cycle]].nil?
            end
            if task[:dependency].resolved?(cycle[:cycle])
              uri=submit_task(localize_task(task,cycle[:cycle]))
              newjobs << {:jobid=>uri, :taskid=>task[:id], :cycle=>cycle[:cycle], :state=>"Submitting", :exit_status=>0, :tries=>1}
            end
          end
        end

        # Harvest job ids for submitted tasks
        newjobs.each do |job|
          uri=job[:jobid]
          jobid,output=get_task_submit_status(job[:jobid])
          if output.nil?
            @logserver.log(job[:cycle],"Submitted #{job[:taskid]}.  Submission status is pending at #{job[:jobid]}")
          else
            if jobid.nil?
              puts output
              @logserver.log(job[:cycle],"Submission of #{job[:taskid]} failed!  #{output}")
            else
              job[:jobid]=jobid
              @logserver.log(job[:cycle],"Submitted #{job[:taskid]}, jobid=#{job[:jobid]}")
            end
          end
        end

        # Add the new jobs to the database
        @workflowdb.add_jobs(newjobs)

      ensure

        # Make sure we release the workflow lock in the database
        @workflowdb.unlock_workflow

        # Make sure to kill the workflow log server 
        @logserver.stop! unless @logserver.nil?

      end
 
    end  # run

  private


    ##########################################
    #
    # build_workflow
    #
    ##########################################
    def build_workflow

      # Get the realtime flag
      @realtime=!(@workflowdoc.realtime.downcase =~ /^t|true$/).nil?

      # Get the cycle life span
      @cyclelifespan=WorkflowMgr.ddhhmmss_to_seconds(@workflowdoc.cyclelifespan)

      # Get the cyclethrottle
      @cyclethrottle=@workflowdoc.cyclethrottle.to_i

      # Get the scheduler
      @scheduler=eval "#{@workflowdoc.scheduler.upcase}BatchSystem.new"

      # Get the log parameters
      @logserver=WorkflowMgr.launchServer("#{@wfmdir}/sbin/workflowlogserver")
      @logserver.setup(WorkflowLog.new(@workflowdoc.log))

      # Get the cycle defs
      @cycledefs=[@workflowdoc.cycledef].flatten.collect do |cycledef|
        fields=cycledef[:cycledef].split
        case fields.size
          when 6
            CycleCron.new(cycledef)
          when 3
            CycleInterval.new(cycledef)
        end
      end

      # Get the tasks 
      @tasks=[]
      @workflowdoc.task.each do |task|
        newtask=task
        task.each do |attr,val|        
          case attr
            when :envar
              newtask[attr].collect! do |envar|
                envar[:name]=CompoundTimeString.new(envar[:name])
                envar[:value]=CompoundTimeString.new(envar[:value])
                envar
              end
            when :dependency
              newtask[attr]=build_dependency(val)
            when :id,:cores
            else
                newtask[attr]=CompoundTimeString.new(val)
          end
        end
        @tasks << newtask
      end

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
            return DataDependency.new(CompoundTimeString.new(nodeval),age)
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
    # activate_new_cycles
    #
    ##########################################
    def activate_new_cycles

      if @realtime
        newcycles=get_new_realtime_cycle
      else
        newcycles=get_new_retro_cycles
      end

      unless newcycles.empty?

        # Add the new cycles to the database
        @workflowdb.add_cycles(newcycles)

        # Add the new cycles to the list of active cycles
        @active_cycles += newcycles.collect { |cycle| {:cycle=>cycle} }

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
      latest_cycle=@workflowdb.get_last_cycle || { :cycle=>Time.gm(1900,1,1,0,0,0) }

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
      dbcycledefs=@workflowdb.get_cycledefs.collect do |dbcycledef|
        case dbcycledef[:cycledef].split.size
          when 6
            CycleCron.new(dbcycledef)
          when 3
            CycleInterval.new(dbcycledef)
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
      cycleset=@workflowdb.get_cycles(@cycledefs.collect { |cycledef| cycledef.position }.compact.min)

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
      @workflowdb.set_cycledefs(@cycledefs.collect { |cycledef| { :group=>cycledef.group, :cycledef=>cycledef.cycledef, :position=>cycledef.position } } )

      return newcycles

    end  # activate_new_cycles


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


    ##########################################
    #
    # submit_task
    #
    ##########################################
    def submit_task(task)

      # Open a double ended pipe
      r,w = IO.pipe

      # Fork a child process to submit the job
      child = fork {

        # Attach the child process's STDOUT to the write end of the pipe
        STDOUT.reopen w

        # Start a Drb Server that will manage the job submission
        DRb.start_service nil,JobSubmitter.new(@scheduler)

        # Write the URI for the job submission server to the pipe so the main
	# thread can get it and use it to connect to the job sumission server process
        STDOUT.puts "#{DRb.uri}"
        STDOUT.flush

        # Allow INT signal to cleanly terminate the server process
        trap("INT") { DRb.stop_service }

        # Wait forever for the server to quit
        DRb.thread.join

      }

      # Close the write end of the pipe in the parent
      w.close

      # Read the URI of the job submission server sent from the child process
      uri=r.gets

      # Connect to the job submission server
      submitter = DRbObject.new nil, uri

      # Send the job submission server a request to submit our job
      submitter.submit(task)

      # Return the URI of the job submission server so we can reconnect to it later
      # to retrieve the job id or error returned from the batch system
      return uri

    end

    ##########################################
    #
    # get_task_submit_status
    #
    ##########################################
    def get_task_submit_status(uri)

      # Connect to the job submission server
      submitter = DRbObject.new(nil, uri)

      # Ask for the output of the submission
      output=submitter.getoutput

      # If there's output, ask for the jobid
      if output.nil?
        jobid=nil
      else
        jobid=submitter.getjobid
      end

      # Stop the job submission server if there was output
      submitter.stop! unless output.nil?

      # Return the jobid and output
      return jobid,output

    end

  end  # Class WorkflowEngine

end  # Module WorkflowMgr
