##########################################
#
# Module WFMStat
#
##########################################
module WFMStat

  ##########################################
  #
  # Class StatusEngine
  #
  ##########################################
  class StatusEngine

    require 'workflowmgr/workflowdoc'
    require 'workflowmgr/workflowstate'
    require 'workflowmgr/workflowdb'
    require 'workflowmgr/cycledef'
    require "workflowmgr/cycle"
    require 'workflowmgr/dependency'
    require 'workflowmgr/workflowconfig'
    require 'workflowmgr/launchserver'
    require 'workflowmgr/dbproxy'
    require 'workflowmgr/workflowioproxy'

    ##########################################
    #
    # initialize
    #
    ##########################################
    def initialize(options)

      begin

        # Disable garbage collection
        GC.disable

        # Turn on full program tracing for verbosity 1000+
        if WorkflowMgr::VERBOSE > 999
          set_trace_func proc { |event,file,line,id,binding,classname| printf "%10s %s:%-2d %10s %8s\n",event,file,line,id,classname }

        # Turn on program tracing for Rocoto code only for verbosity 100+
        elsif WorkflowMgr::VERBOSE > 99
          set_trace_func proc { |event,file,line,id,binding,classname|
            case event
              when "call","return","line"
                if file=~/\/lib\/workflowmgr\/|\/lib\/wfmstat\//
                  printf "%10s %s:%-2d %10s %8s\n",event,file,line,id,classname
                end
              else
            end
          }
        end

        # Get configuration file options
        @config=WorkflowMgr::WorkflowYAMLConfig.new

        # Get command line options
        @options=options

        # Set up an object to serve the workflow database (but do not open the database)
        @dbServer=WorkflowMgr::DBProxy.new(@config,@options)

      rescue => crash
        WorkflowMgr.stderr(crash.message,1)
        WorkflowMgr.log(crash.message)
        case
          when crash.is_a?(ArgumentError),crash.is_a?(NameError),crash.is_a?(TypeError)
            WorkflowMgr.stderr(crash.backtrace.join("\n"),1)
            WorkflowMgr.log(crash.backtrace.join("\n"))
          else
        end
        Process.exit(1)
      end

    end  # initialize


    ##########################################
    #
    # wfmstat
    #
    ##########################################
    def wfmstat

      begin

        # Open/Create the database
        @dbServer.dbopen({:readonly=>true})

        # Set up an object to serve file stat info
        @workflowIOServer=WorkflowMgr::WorkflowIOProxy.new(@dbServer,@config,@options)

        # Open the workflow document
        @workflowdoc = WorkflowMgr::WorkflowXMLDoc.new(@options.workflowdoc,@workflowIOServer,@config)

        @workflowdoc.features_supported?

        # Get the task and cycle subsets
        @subset=@options.selection.make_subset(tasks=@workflowdoc.tasks,cycledefs=@workflowdoc.cycledefs,dbServer=@dbServer)

        # Print a cycle summary report if requested
        if @options.summary
          print_summary
        else
          print_status
        end

      rescue => crash
        WorkflowMgr.stderr(crash.message,1)
        WorkflowMgr.log(crash.message)
        case
          when crash.is_a?(ArgumentError),crash.is_a?(NameError),crash.is_a?(TypeError)
            WorkflowMgr.stderr(crash.backtrace.join("\n"),1)
            WorkflowMgr.log(crash.backtrace.join("\n"))
          else
        end
        Process.exit(1)

      ensure

        # Make sure we release the workflow lock in the database and shutdown the dbserver
        unless @dbServer.nil?
          @dbServer.stop! if @config.DatabaseServer
        end

        # Make sure to shut down the workflow file stat server
        unless @workflowIOServer.nil?
          @workflowIOServer.stop! if @config.WorkflowIOServer
        end

      end  # ensure

    end  # wfmstat

    ##########################################
    #
    # checkOneTask
    #
    ##########################################
    def checkOneTask(cycletime,taskname,cycledefs)
      # Get the cycle
      cycle=@dbServer.get_cycles( {:start=>cycletime, :end=>cycletime } ).first || WorkflowMgr::Cycle.new(cycletime)

      # Get the task
      task=@workflowdoc.tasks[taskname]
      task=task.localize(cycletime) unless task.nil?

      # Get the job (if there is one)
      jobcycles=[cycletime]
      @workflowdoc.taskdep_cycle_offsets.each do |offset|
        jobcycles << cycletime + offset
      end
      jobs=@dbServer.get_jobs(jobcycles)
      if jobs[taskname].nil?
        job=nil
      else
        job=jobs[taskname][cycletime]
      end

      # Print the task information
      print_taskinfo(task)

      # Query and print task dependency info
      dependencies=nil
      hangdependencies=nil
      unless task.nil?
        unless task.dependency.nil?
          wstate=WorkflowMgr::WorkflowState.new(cycle.cycle,jobs,@workflowIOServer,@workflowdoc.cycledefs,task.attributes[:name],task,tasks=@workflowdoc.tasks)
          dependencies=task.dependency.query(wstate)
          printf "%2s%s\n", "","dependencies"
          print_deps(dependencies,0)
        end
        unless task.hangdependency.nil?
          wstate=WorkflowState.new(cycle.cycle,jobs,@workflowIOServer,@workflowdoc.cycledefs,task.attributes[:name],task,tasks=@workflowdoc.tasks)
          hangdependencies=task.hangdependency.query(wstate)
          printf "%2s%s\n", "","hang dependencies"
          print_deps(hangdependencies,0)
        end
      end

      # Print the cycle information
      print_cycleinfo(cycle,cycledefs,task)

      # Print the job information
      print_jobinfo(job)

      # Print throttling violations
      print_violations(task,cycle,dependencies) if job.nil?

    end

    ##########################################
    #
    # checkTasks
    #
    ##########################################
    def checkTasks

      begin

        # Open/Create the database
        @dbServer.dbopen({:readonly=>true})

        # Set up an object to serve file stat info
        @workflowIOServer=WorkflowMgr::WorkflowIOProxy.new(@dbServer,@config,@options)

        # Open the workflow document
        @workflowdoc = WorkflowMgr::WorkflowXMLDoc.new(@options.workflowdoc,@workflowIOServer,@config)

        @subset=@options.selection.make_subset(tasks=@workflowdoc.tasks,cycledefs=@workflowdoc.cycledefs,dbServer=@dbServer)

        cycledefs=@workflowdoc.cycledefs

        @subset.each_cycle do |cycletime|
          @subset.each_task do |taskname|
            checkOneTask(cycletime,taskname,cycledefs)
          end
        end

      rescue => crash
        WorkflowMgr.stderr(crash.message,1)
        WorkflowMgr.log(crash.message)
        case
          when crash.is_a?(ArgumentError),crash.is_a?(NameError),crash.is_a?(TypeError)
            WorkflowMgr.stderr(crash.backtrace.join("\n"),1)
            WorkflowMgr.log(crash.backtrace.join("\n"))
          else
        end
        Process.exit(1)

      ensure

        # Make sure we release the workflow lock in the database and shutdown the dbserver
        unless @dbServer.nil?
          @dbServer.stop! if @config.DatabaseServer
        end

        # Make sure to shut down the workflow file stat server
        unless @workflowIOServer.nil?
          @workflowIOServer.stop! if @config.WorkflowIOServer
        end

      end  # ensure

    end

    ##########################################
    #
    # getCycles
    #
    ##########################################
    def getCycles

      # Turn the db, xml, and undef iterators into arrays:
      dbcycles=@subset.collect_db_cycles(){|c|c}
      xmlcycles=@subset.collect_xml_cycles(){|c|c}
      undefcycles=@subset.collect_undef_cycles(){|c|c}

      return [dbcycles,xmlcycles,undefcycles]
    end


    ##########################################
    #
    # print_summary
    #
    ##########################################
    def print_summary

      # Get cycles of interest
      dbcycles,xmlcycles,undefcycles=getCycles

      # Print the header
      printf "%12s    %8s    %20s    %20s\n","CYCLE".center(12),
                                             "STATE".center(8),
                                             "ACTIVATED".center(20),
                                             "DEACTIVATED".center(20)

      # Print the cycle date/times
      (dbcycles+xmlcycles).sort.each do |cycle|
        printf "%12s    %8s    %20s    %20s\n","#{cycle.cycle.strftime("%Y%m%d%H%M")}",
                                               "#{cycle.state.to_s.capitalize}",
                                               "#{cycle.activated_time_string.center(20)}",
                                               "#{cycle.deactivated_time_string.center(20)}"
      end

    end

    ##########################################
    #
    # print_status
    #
    ##########################################
    def print_status

      # Get cycles of interest
      dbcycles,xmlcycles,undefcycles=getCycles

      # Get the jobs from the database for the cycles of interest
      jobs=@dbServer.get_jobs(dbcycles.collect {|c| c.cycle})

      # Get the list of tasks from the workflow definition
      definedTasks=@workflowdoc.tasks

      # Get the cycle defs
      cycledefs=@workflowdoc.cycledefs

      # Initialize empty hash of task cycledefs
      taskcycledefs={}

      # Print the job status info
      if @options.taskfirst

        format = "%20s    %12s    %24s    %16s    %16s    %6s    %10s\n"
        header = "TASK".rjust(20),"CYCLE".rjust(12),"JOBID".rjust(24),
                 "STATE".rjust(16),"EXIT STATUS".rjust(16),"TRIES".rjust(6),
                 "DURATION".rjust(10)
        puts format % header

        # Sort the task list in sequence order
        tasklist=jobs.keys | definedTasks.values.collect { |t| t.attributes[:name] }
        tasklist=tasklist.sort_by { |t| [definedTasks[t].nil? ? 999999999 : definedTasks[t].seq, t.split(/(\d+)/).map { |i| i=~/\d+/ ? i.to_i : i }].flatten }

        tasklist.each do |task|

          next unless @subset.is_selected? task

          printf "================================================================================================================================\n"

          # Print status of all jobs for this task
          cyclelist=(dbcycles | xmlcycles).collect { |c| c.cycle }.sort
          cyclelist.each do |cycle|

            next unless @subset.is_selected? cycle

            # Only print info if the cycle is defined for this task
            unless definedTasks[task].attributes[:cycledefs].nil?
              # Get the cycledefs associated with this task
              taskcycledefs[task]=cycledefs.find_all { |cycledef| definedTasks[task].attributes[:cycledefs].split(/[\s,]+/).member?(cycledef.group) }
              # Reject this task if the cycle is not a member of the tasks cycle list
              next unless taskcycledefs[task].any? { |cycledef| cycledef.member?(cycle) }
            end

            if jobs[task].nil?
              jobdata=["-","-","-","-","-"]
            elsif jobs[task][cycle].nil?
              jobdata=["-","-","-","-","-"]
            else
              case jobs[task][cycle].state
                when "SUCCEEDED","DEAD","FAILED"
                  jobdata=[jobs[task][cycle].id,jobs[task][cycle].state,jobs[task][cycle].exit_status,jobs[task][cycle].tries,jobs[task][cycle].duration]
                else
                  jobdata=[jobs[task][cycle].id,jobs[task][cycle].state,"-",jobs[task][cycle].tries,jobs[task][cycle].duration]
              end
            end
            puts format % ([task,cycle.strftime("%Y%m%d%H%M")] + jobdata)
          end
        end

     else

        format = "%12s    %20s    %24s    %16s    %16s    %6s    %10s\n"
        header = "CYCLE".rjust(12),"TASK".rjust(20),"JOBID".rjust(24),
                 "STATE".rjust(16),"EXIT STATUS".rjust(16),"TRIES".rjust(6),
                 "DURATION".rjust(10)
        puts format % header

        # Print status of jobs for each cycle
        cyclelist=(dbcycles | xmlcycles).collect { |c| c.cycle }.sort
        cyclelist.each do |cycle|

          if ! @subset.is_selected? cycle
            #puts "#{cycle.class.name} #{cycle.inspect}: not selected"
            next
          end

          printf "================================================================================================================================\n"

          # Sort the task list in sequence order
          tasklist=jobs.keys | definedTasks.values.collect { |t| t.attributes[:name] }
          tasklist=tasklist.sort_by { |t| [definedTasks[t].nil? ? 999999999 : definedTasks[t].seq, t.split(/(\d+)/).map { |i| i=~/\d+/ ? i.to_i : i }].flatten }
          tasklist.each do |task|

            if ! @subset.is_selected? task
              #puts "#{task}: not selected"
              next
            end

            # Only print info if the task is defined for this cycle
            unless definedTasks[task].nil? or definedTasks[task].attributes[:cycledefs].nil?
              # Get the cycledefs associated with this task
              taskcycledefs[task]=cycledefs.find_all { |cycledef| definedTasks[task].attributes[:cycledefs].split(/[\s,]+/).member?(cycledef.group) }
              # Reject this task if the cycle is not a member of the tasks cycle list
              next unless taskcycledefs[task].any? { |cycledef| cycledef.member?(cycle) }
            end

            if jobs[task].nil?
              jobdata=["-","-","-","-","-"]
            elsif jobs[task][cycle].nil?
              jobdata=["-","-","-","-","-"]
            else
              case jobs[task][cycle].state
                when "SUCCEEDED","DEAD","FAILED"
                  jobdata=[jobs[task][cycle].id,jobs[task][cycle].state,jobs[task][cycle].exit_status,jobs[task][cycle].tries,jobs[task][cycle].duration]
                else
                  jobdata=[jobs[task][cycle].id,jobs[task][cycle].state,"-",jobs[task][cycle].tries,jobs[task][cycle].duration]
              end
            end
            puts format % ([cycle.strftime("%Y%m%d%H%M"),task] + jobdata)
          end
        end

      end

    end


    ##########################################
    #
    # print_taskinfo
    #
    ##########################################
    def print_taskinfo(task)

      puts
      if task.nil?
        puts "Task: Not defined in current workflow definition"
      else
        puts "Task: #{task.attributes[:name]}"
        task.attributes.keys.sort { |a1,a2| a1.to_s <=> a2.to_s }.each { |attr|
          puts "  #{attr}: #{task.attributes[attr]}"
        }
        puts "  environment"
        task.envars.keys.sort.each { |envar|
          puts "    #{envar} ==> #{task.envars[envar]}"
        }
      end

    end


    ##########################################
    #
    # print_cycleinfo
    #
    ##########################################
    def print_cycleinfo(cycle,cycledefs,task)

      # Make sure the cycle is valid for this task
      cycle_is_valid=true
      unless task.attributes[:cycledefs].nil?
        taskcycledefs=cycledefs.find_all { |cycledef| task.attributes[:cycledefs].split(/[\s,]+/).member?(cycledef.group) }
        # Cycle is invalid for this task if the cycle is not a member of the tasks cycle list
        unless taskcycledefs.any? { |cycledef| cycledef.member?(cycle.cycle) }
          cycle_is_valid=false
        end
      end  # unless

      puts
      puts "Cycle: #{cycle.cycle.strftime("%Y%m%d%H%M")}"
      if cycle_is_valid
        puts "  Valid for this task: YES"
      else
        puts "  Valid for this task: NO"
      end
      puts "  State: #{cycle.state}"
      puts "  Activated: #{cycle.activated != Time.at(0) ? cycle.activated : "-"}"
      puts "  Completed: #{cycle.done? ? cycle.done : "-"}"
      puts "  Expired: #{cycle.expired? ? cycle.expired : "-"}"

    end


    ##########################################
    #
    # print_jobinfo
    #
    ##########################################
    def print_jobinfo(job)

      puts
      if job.nil?
        puts "Job: This task has not been submitted for this cycle"
      else
        puts "Job: #{job.id}"
        puts "  State:  #{job.state} (#{job.native_state})"
        puts "  Exit Status:  #{job.done? ? job.exit_status : "-"}"
        puts "  Tries:  #{job.tries}"
        puts "  Unknown count:  #{job.nunknowns}"
        puts "  Duration:  #{job.duration}"
      end

    end


    ##########################################
    #
    # print_violations
    #
    ##########################################
    def print_violations(task,cycle,dependencies)

      puts
      puts "Task can not be submitted because:"

      # Check for non-existent task
      if task.nil?
        puts "  The task is not defined"
        return
      end

      # Check for inactive cycle
      unless cycle.active?
        puts "  The cycle is not active"
        return
      end

      # Check for unsatisfied dependencies
      unless dependencies.nil?
        unless dependencies.first[:resolved]
          puts "  Dependencies are not satisfied"
          return
        end
      end

      # Check for throttle violations
      active_cycles=@dbServer.get_active_cycles
      active_jobs=@dbServer.get_jobs(active_cycles.collect { |c| c.cycle })
      ncores=0
      ntasks=0
      active_jobs.keys.each do |jobtask|
        active_jobs[jobtask].keys.each do |jobcycle|
           if !active_jobs[jobtask][jobcycle].done?
            ntasks += 1
            ncores += active_jobs[jobtask][jobcycle].cores
          end
        end
      end
      unless @workflowdoc.taskthrottle.nil?
        if ntasks + 1 > @workflowdoc.taskthrottle
          puts "  Task throttle violation (#{ntasks} of #{@workflowdoc.taskthrottle} tasks are already active)"
        end
      end
      unless @workflowdoc.corethrottle.nil?
        if ncores + task.attributes[:cores] > @workflowdoc.corethrottle
          puts "  Core throttle violation (#{ncores} of #{@workflowdoc.corethrottle} cores are already in use)"
        end
      end

    end


    ##########################################
    #
    # print_deps
    #
    ##########################################
    def print_deps(deps,n)

      return if deps.nil?
      deps.each do |d|
        if d.is_a?(Array)
          print_deps(d,n+1) if d.is_a?(Array)
        else
          printf "%#{2*n+4}s%s %s\n","",d[:dep],d[:msg]
        end
      end

    end


  end  # Class StatusEngine

end  # Module WorkflowMgr
