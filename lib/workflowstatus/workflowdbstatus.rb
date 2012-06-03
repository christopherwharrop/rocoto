##########################################
#
# Module WorkflowMgr
#
##########################################
module WorkflowMgr

  ##########################################
  #
  # Class TaskTable
  #
  ##########################################
  class TaskTable

    attr_reader :taskname

    def initialize(taskname)
      @taskname = taskname   # String
      @cyclelist = []        # list of Cycle objects
    end

    def add_cycle(cycle)
      @cyclelist << cycle
    end

    # sort Cycle objects in place by time
    def sort!
      @cyclelist.sort!
    end

    def print(cycles)
      ## print out info, if cycle matches input_cycles
      cycles.each do |ic|
        puts "checking #{ic}..."
        index = nil
        @cyclelist.each_with_index do |sc,i|
          index = i if ic == sc.time
        end
        unless (index.nil?) then
          puts "  TASKNAME:  #{@taskname}  #{@cyclelist[index]}"
        end
      end        
    end

  end  # TaskTable


  ##########################################
  #
  # Class Cycle
  #
  ##########################################
  class Cycle

    include Enumerable

    attr_reader :time, :state

    def initialize(time,state)
      @time = time
      @state = state
    end

    # sort by time
    def <=>(other)
      @time <=> other.time
    end

    def to_s
      "   CYCLE:  #{@time}   STATE:  #{@state}"
    end

  end  # Cycle


  ##########################################
  #
  # Class WorkflowDbStatus
  #
  ##########################################
  class WorkflowDbStatus


    require 'drb'
    require 'workflowmgr/workflowdoc'
    require 'workflowmgr/workflowdb'
    require 'workflowmgr/cycledef'
    require 'workflowmgr/dependency'
    require 'workflowstatus/workflowstatusopt'

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
      @options=WorkflowStatusOpt.new(args)

      puts @options.cycles.inspect

      # Get configuration file options
      @config=WorkflowYAMLConfig.new

      # Get the base directory of the WFM installation
      @wfmdir=File.dirname(File.dirname(File.expand_path(File.dirname(__FILE__))))

      # Set up an object to serve the workflow database (but do not open the database)
      @dbServer=DBProxy.new(@options.database,@config)

      # Set up an object to serve file stat info
      @workflowIOServer=WorkflowIOProxy.new(@dbServer,@config)        

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

        # Get all cycles  (array of hashes)
        #     [:cycle, :activated, :expired, :done]
        puts "========================="
        puts "         CYCLES          "
        puts "========================="

        array_cycles = @dbServer.get_cycles
        print "Length of array is:  ", array_cycles.length, "\n"
        print "Keys:  ", array_cycles[0].keys.inspect, "\n\n"
#        array_cycles.each do |cycle| 
#          puts "CYCLE   #{cycle[:cycle]}"
#          puts "   Activated:  #{cycle[:activated]}"
#          puts "   Expired:    #{cycle[:expired]}"
#          puts "   Done:       #{cycle[:done]}"
#        end

        # get all jobs  (hash of hash of hashes)
##          ["ungrib_NAM", "post_nmm_000", "post_nmm_003", "post_nmm_006", "post_nmm_009", 
##           "real_nmm", "metgrid_nmm", "post_nmm_021", "post_nmm024", "post_nmm_027", 
##           "wrf_nmm", "post_nmm_042", "post_nmm_045", "post_nmm_048", "post_nmm_012", 
##           "post_nmm_015", "post_nmm_018", "post_nmm_030", "post_nmm_033", "post_nmm_036", 
##           "post_nmm_039"]
        puts "========================="
        puts "          JOBS          "
        puts "========================="

        all_tasks = @dbServer.get_jobs
        keys = all_tasks.keys
        print "Length of hash is:  ", all_tasks.length, "\n\n"
        print "Job Keys:  ", keys.inspect, "\n\n"
        task_keys = all_tasks[keys.first].keys
#        puts "  Task Keys are the cycle dates; Task Values are a hash"
        cycle_keys = all_tasks[keys.first][task_keys.first].keys
        print "    Cycle Keys:  ", cycle_keys.inspect, "\n\n"

        tasktables = []
        all_tasks.each do |taskname, task_value|     ### for each task, the task value is the cycle
          puts "TASK NAME:   #{taskname}"

##
##        [:cycle, :state, :taskname, :tries, :exit_status, :nunknowns, :cores, :jobid]
##
          tasktable = TaskTable.new(taskname)
          task_value.each do |cycle_key, cycle_value| 
#            puts "   CYCLE:   #{cycle_key}   STATE:  : #{cycle_value[:state]}" 
            tasktable.add_cycle(Cycle.new(cycle_key,cycle_value[:state]))
          end
          tasktable.sort!
          tasktables << tasktable
        end

        tasktables.each do |tasktable|
          tasktable.print(@options.cycles)
          # tasktable.print(@options.tasks,@options.cycles)
        end
          



        # open workflow document and get list of tasks from XML file
        #    tasks is an array of Task objects
        #       attributes is a hash
        #           keys:  account, command, maxtries, name, jobname, cores, queue, native, 
        #                  cycledefs, walltime, join, memory
        #       dependency is an Dependency object
        #       envars is 
        #

        puts "========================="
        puts "=========TASKS=========="
        puts "========================="

        tasks = WorkflowXMLDoc.new(@options.workflowdoc, @workflowIOServer).tasks

        puts tasks.class
        puts tasks.length
        puts tasks[0].class
        puts "************"
        
        ### attributes
        tasks.each do |task|
          puts "++++++++++++"
          puts "Attributes:  #{task.attributes.keys.inspect}"
          task.attributes.each do |attr_key, attr_val|
            if attr_val.class == CompoundTimeString then 
              puts "  #{attr_key}   #{attr_val.to_s(Time.now)}"
            else
              puts "  #{attr_key}   #{attr_val}"
            end
          end
        puts "************"
        end
        
        ### dependencies
        tasks.each do |task|
          puts "++++++++++++"
          puts "Dependency:  #{task.dependency.inspect}"
          puts task.dependency.class
          puts "************"
        end

        ### environment variables
        tasks.each do |task|
          puts "++++++++++++"
          puts "Dependency:  #{task.envars.inspect}"
          puts task.envars.class
          puts "************"
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

      end
    end  # run

  end  # Class WorkflowDbStatus

end  # Module WorkflowMgr
