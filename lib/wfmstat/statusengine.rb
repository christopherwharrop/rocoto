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
    require 'workflowmgr/workflowdb'
    require 'workflowmgr/cycledef'
    require "workflowmgr/cycle"
    require 'workflowmgr/dependency'
    require 'wfmstat/statusoption'
#    require 'wfmstat/summarytable'
#    require 'wfmstat/jobtables'
#    require 'wfmstat/job'

    require 'workflowmgr/workflowconfig'
    require 'workflowmgr/launchserver'
    require 'workflowmgr/dbproxy'
    require 'workflowmgr/workflowioproxy'

    ##########################################
    #
    # initialize
    #
    ##########################################
    def initialize(args)

      # Get command line options
      @options=StatusOption.new(args)

      # Get configuration file options
      @config=WorkflowMgr::WorkflowYAMLConfig.new

      # Get the base directory of the WFM installation
      @wfmdir=File.dirname(File.dirname(File.expand_path(File.dirname(__FILE__))))

      # Set up an object to serve the workflow database (but do not open the database)
      @dbServer=WorkflowMgr::DBProxy.new(@options.database,@config)

      # Set up an object to serve file stat info
      @workflowIOServer=WorkflowMgr::WorkflowIOProxy.new(@dbServer,@config)        

      # Open the workflow document
      @workflowdoc = WorkflowMgr::WorkflowXMLDoc.new(@options.workflowdoc,@workflowIOServer)

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

        # Print a cycle summary report if requested
        if @options.summary
          print_summary
        else
          print_status
        end
    
      ensure
  
        # Make sure we release the workflow lock in the database and shutdown the dbserver
        unless @dbServer.nil?
          @dbServer.unlock_workflow if @locked
          @dbServer.stop! if @config.DatabaseServer
        end
  
        # Make sure to shut down the workflow file stat server
        unless @workflowIOServer.nil?
          @workflowIOServer.stop! if @config.WorkflowIOServer
        end
  
      end  # ensure

    end  # run


    ##########################################
    #
    # getCycles
    #
    ##########################################
    def getCycles

      # Initialize empty lists of cycles
      dbcycles=[]
      xmlcycles=[]
      undefcycles=[]

      # Get the cycles of interest that are in the database
      if @options.cycles.nil?
        # Get the latest cycle
        dbcycles << @dbServer.get_last_cycle
      elsif @options.cycles.is_a?(Range)
        # Get all cycles within the range
        dbcycles += @dbServer.get_cycles( {:start=>@options.cycles.first, :end=>@options.cycles.last } )
      elsif @options.cycles.is_a?(Array)
        # Get the specific cycles asked for
        @options.cycles.each do |c|
          cycle = @dbServer.get_cycles( {:start=>c, :end=>c } )
          if cycle.empty?
            undefcycles << WorkflowMgr::Cycle.new(c)
          else
            dbcycles += cycle
          end
        end
      else
        puts "Invalid cycle specification"
      end
      
      # Add cycles defined in XML that aren't in the database
      # We only need to do this when a range of cycles is requested
      if @options.cycles.is_a?(Range)

        # Get the cycle definitions
        cycledefs = @workflowdoc.cycledefs

        # Find every cycle in the range
        xml_cycle_times = []
        reftime=cycledefs.collect { |cdef| cdef.next(@options.cycles.first) }.compact.min
        while true do
          break if reftime.nil?
          break if reftime > @options.cycles.last
          xml_cycle_times << reftime
          reftime=cycledefs.collect { |cdef| cdef.next(reftime+60) }.compact.min
        end

        # Add the cycles that are in the XML but not in the DB
        xmlcycles = (xml_cycle_times - dbcycles.collect { |c| c.cycle } ).collect { |c| WorkflowMgr::Cycle.new(c) }

      end

      [dbcycles,xmlcycles,undefcycles]

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

      # Print the job status info
      if @options.taskfirst

        format = "%20s    %12s    %14s    %16s    %16s    %6s\n"
        header = "TASK".rjust(20),"CYCLE".rjust(12),"JOBID".rjust(14),
                 "STATE".rjust(16),"EXIT STATUS".rjust(16),"TRIES".rjust(6)
        puts format % header

        # Sort the task list in sequence order
        tasklist=jobs.keys | definedTasks.values.collect { |t| t.attributes[:name] }
        unless @options.tasks.nil?
          tasklist = tasklist.find_all { |task| @options.tasks.any? { |pattern| task=~/#{pattern}/ } }
        end
        tasklist=tasklist.sort { |t1,t2| definedTasks[t1].seq <=> definedTasks[t2].seq }

        tasklist.each do |task|

          printf "========================================================================================================\n"

          # Print status of all jobs for this task
          cyclelist=(dbcycles | xmlcycles).collect { |c| c.cycle }.sort
          cyclelist.each do |cycle|
            if jobs[task].nil?
              jobdata=["-","-","-","-"]
            elsif jobs[task][cycle].nil?
              jobdata=["-","-","-","-"]
            else
              jobdata=[jobs[task][cycle].id,jobs[task][cycle].state,jobs[task][cycle].exit_status,jobs[task][cycle].tries]
            end
            puts format % ([task,cycle.strftime("%Y%m%d%H%M")] + jobdata)
          end
        end
 
     else 

        format = "%12s    %20s    %14s    %16s    %16s    %6s\n"
        header = "CYCLE".rjust(12),"TASK".rjust(20),"JOBID".rjust(14),
                 "STATE".rjust(16),"EXIT STATUS".rjust(16),"TRIES".rjust(6)
        puts format % header

        # Print status of jobs for each cycle
        cyclelist=(dbcycles | xmlcycles).collect { |c| c.cycle }.sort
        cyclelist.each do |cycle|

          printf "========================================================================================================\n"

          # Sort the task list in sequence order 
          tasklist=jobs.keys | definedTasks.values.collect { |t| t.attributes[:name] }
          unless @options.tasks.nil?
            tasklist = tasklist.find_all { |task| @options.tasks.any? { |pattern| task=~/#{pattern}/ } }
          end
          tasklist=tasklist.sort { |t1,t2| definedTasks[t1].seq <=> definedTasks[t2].seq }
          tasklist.each do |task|
            if jobs[task].nil?
              jobdata=["-","-","-","-"]
            elsif jobs[task][cycle].nil?
              jobdata=["-","-","-","-"]
            else
              jobdata=[jobs[task][cycle].id,jobs[task][cycle].state,jobs[task][cycle].exit_status,jobs[task][cycle].tries]
            end
            puts format % ([cycle.strftime("%Y%m%d%H%M"),task] + jobdata)
          end
        end

      end      

    end

  end  # Class StatusEngine

end  # Module WorkflowMgr
