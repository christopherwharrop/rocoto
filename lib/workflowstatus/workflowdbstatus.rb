##########################################
#
# Module WorkflowMgr
#
##########################################
module WorkflowMgr

  ##########################################
  #
  # Class TaskTables
  #
  ##########################################
  class TaskTables

    attr_reader :tasktables

    def initialize
      @tasktables = []
    end

    def << (tasktable)
      @tasktables << tasktable
    end

    # sort TaskTable objects in place by task name
    def sort!
      @tasktables.sort!
    end

    # print with tasknames sorted 
    #    if no taskname argument list, print out all tasks
    #    if specified taskname does not exist, print nothing
    def print(tasknames_arglist, cycles_arglist_string, taskfirst)

      sort!

      ## print header line
      if taskfirst == true then
        header_format = "%18s %22s %10s %16s %12s %6s\n"
        header_string = "TASK".center(18),"CYCLE".center(18),"JOBID".center(10), 
                        "STATE".center(16),"EXIT STATUS".center(12),"TRIES".center(6)
      else
        header_format = "%22s %18s %10s %16s %12s %6s\n"
        header_string = "CYCLE".center(18),"TASK".center(18),"JOBID".center(10), 
                        "STATE".center(16),"EXIT STATUS".center(12),"TRIES".center(6)
      end
      header = header_format % header_string
      puts header

      ## print out info, if task matches input_taskname
      if tasknames_arglist.empty? then
        @tasktables.each do |tasktable|
          tasktable.print(cycles_arglist_string,taskfirst)
        end
      else
        tasknames_arglist.sort.each do |it|
          @tasktables.each do |tasktable|
            if it == tasktable.taskname then
              tasktable.print(cycles_arglist_string,taskfirst)
            end
          end
        end
      end
    end  # def print

  end  # TaskTables


  ##########################################
  #
  # Class TaskTable
  #
  ##########################################
  class TaskTable

    include Enumerable

    attr_reader :taskname

    def initialize(taskname)
      @taskname = taskname   # String
      @cyclelist = []        # list of Cycle objects
    end

    def add_cycle(cycle)
      @cyclelist << cycle
    end

    # sort by taskname
    def <=>(other)
      @taskname <=> other.taskname
    end

    # sort Cycle objects in place by time
    def sort!
      @cyclelist.sort!
    end

    # print with cycles sorted by time
    #    if no cycle argument list, print out latest cycle activated
    def print(cycles_arglist_string, taskfirst)

      sort!

      # range of cycles
      if cycles_arglist_string.include?(':') then
        index = cycles_arglist_string.index(':')
        if index == 0 then                                        ## :c2
          first = '190001010000'
          last  = cycles_arglist_string[index.next..cycles_arglist_string.length-1]
        elsif index == cycles_arglist_string.length-1 then        ## c1:
          first = cycles_arglist_string[0..index-1]
          last =  '999912311259'
        else                                                      ## c1:c2
          first = cycles_arglist_string[0..index-1]
          last  = cycles_arglist_string[index.next..cycles_arglist_string.length-1]
        end

        # convert to array of Time objects
        cycles_range = []
        crange = [first, last]
        crange.each do |cyclestr|
          parsed_date = ParseDate.parsedate(cyclestr.strip)
          tm = Time.utc(parsed_date[0], parsed_date[1], parsed_date[2], parsed_date[3], 
                        parsed_date[4])
          cycles_range << tm
        end

        # print out info for specified cycles if first <= cycle <= last 
        @cyclelist.each_with_index do |sc,i|
          index= nil
          index = i if sc.time.between?(cycles_range[0],cycles_range[1])
          if (!index.nil?) then
            puts "  #{@taskname.ljust(18)} #{@cyclelist[index]}"
          end
        end

      # list of cycles or last cycle, if none specified
      else
        cycles_arglist = []
        cycles_arglist_string.split(',').each do |cyclestr|
          parsed_date = ParseDate.parsedate(cyclestr.strip)
          tm = Time.utc(parsed_date[0], parsed_date[1], parsed_date[2], parsed_date[3], 
                        parsed_date[4])
          cycles_arglist << tm
        end

        # -c option not specified
        if cycles_arglist.empty? then
          cycles = [] << @cyclelist.last               ## match last cycle activated
        else
          cycles = cycles_arglist.sort
        end
  
        # print out info for specified cycles if cycle matches input_cycle
        cycles.each do |ic|
#DEBUG        puts "checking #{ic}...   Taskname:  #{@taskname}"
          index = nil
          @cyclelist.each_with_index do |sc,i|
            index = i if ic == sc.time
          end
          if (index.nil?) then
            if taskfirst == true then
              cycle_string = sprintf("%18s %18s", "  #{@taskname.ljust(18)}", "#{ic.strftime("%b %d %Y %H:%M").center(18)}")
            else
              cycle_string = sprintf("%18s %18s","  #{ic.strftime("%b %d %Y %H:%M").center(18)}", "#{@taskname.ljust(18)}")
            end
            info_string  =  sprintf("%11s %16s %9s %8s", "-","-","-","-")
          else
            if taskfirst == true then
              cycle_string = sprintf("%18s %18s", "  #{@taskname.ljust(18)}", 
                                     "#{@cyclelist[index].time.strftime("%b %d %Y %H:%M").center(18)}")
            else
              cycle_string = sprintf("%18s %18s", 
                                     "  #{@cyclelist[index].time.strftime("%b %d %Y %H:%M").center(18)}", 
                                     "#{@taskname.ljust(18)}")
            end
            info_string  =  sprintf("%11s %16s %9s %8s", "#{@cyclelist[index].jobid.to_s[0,10]}", 
                                    "#{@cyclelist[index].state.rjust(16)}", 
                                    "#{@cyclelist[index].exit_status.to_s[0,5]}", "#{@cyclelist[index].tries.to_s[0,6]}")
          end
	  output_string = cycle_string + info_string
          puts output_string
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

    attr_reader :time, :state, :jobid, :exit_status, :tries

    def initialize(time,jobid,state,exit_status,tries)
      @time = time
      @jobid = jobid
      @state = state
      @exit_status = exit_status
      @tries = tries
    end

    # sort by time
    def <=>(other)
      @time <=> other.time
    end

    def to_s
      sprintf("%18s %10s %16s %9s %8s", "#{@time.strftime("%b %d %Y %H:%M").center(18)}", "#{@jobid.to_s[0,10]}", "#{@state.rjust(16)}", 
              "#{@exit_status.to_s[0,5]}", "#{@tries.to_s[0,6]}")
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
      puts @options.summary.inspect

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

        # open workflow document 
        workflowdoc = WorkflowXMLDoc.new(@options.workflowdoc, @workflowIOServer)

        # Get all cycles from database (array of hashes)
        #     [:cycle, :activated, :expired, :done]
        array_cycles = @dbServer.get_cycles
        db_cycle_times = []                                     # array of database cycle times
        array_cycles.each do |cycle|
          db_cycle_times << cycle[:cycle]
        end
        puts "!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!"
        puts "DB CYCLES:  #{db_cycle_times.length}"
        puts db_cycle_times.inspect

        ### ======================
        ###     CYCLE SUMMARY          
        ### ======================
        if @options.summary == true then
  
          # print header
          printf "%13s %10s %26s %24s %24s\n","CYCLE".center(12),"STATE".center(8),"ACTIVATED".center(24),
                 "DEACTIVATED".center(24),"DONE".center(24)

          # print cycle date/times
          array_cycles.each do |cycle| 
            printf "%12s %10s %24s %24s %24s\n","#{cycle[:cycle].strftime("%Y%m%d%H%M")}", " state ", 
                                                "#{cycle[:activated].strftime("%b %d %Y %H:%M:%S")}",
                                                "#{cycle[:expired].strftime("%b %d %Y %H:%M:%S")}",
                                                "#{cycle[:done].strftime("%b %d %Y %H:%M:%S")}"
          end

        else
  
          # get all jobs from database (hash of hash of hashes:  [Task][Cycle][Cycle_hash])
  ##          ["ungrib_NAM", "post_nmm_000", "post_nmm_003", "post_nmm_006", "post_nmm_009", 
  ##           "real_nmm", "metgrid_nmm", "post_nmm_021", "post_nmm024", "post_nmm_027", 
  ##           "wrf_nmm", "post_nmm_042", "post_nmm_045", "post_nmm_048", "post_nmm_012", 
  ##           "post_nmm_015", "post_nmm_018", "post_nmm_030", "post_nmm_033", "post_nmm_036", 
  ##           "post_nmm_039"]
          puts "========================="
          puts "          JOBS          "
          puts "========================="
  
          all_tasks = @dbServer.get_jobs
          tasknames = all_tasks.keys
          print "Length of hash is:  ", all_tasks.length, "\n\n"
          print "Job Keys:  ", tasknames.inspect, "\n\n"
          task_db_cycle_times = all_tasks[tasknames.first].keys       ### Task Keys are the cycle dates; Task Values are a hash"
                                                                   ###    assumed all tasks had the same number of cycles
                                                                   ###    not true!!  
                                                                   ###    .first takes the first taskname in hash, which 
                                                                   ###      seems to vary from run to run
          print tasknames.first
          print "    Task Keys (#{task_db_cycle_times.length}):  ", task_db_cycle_times.inspect, "\n\n"
          cycle_keys = all_tasks[tasknames.first][task_db_cycle_times.first].keys        ### uses .first since same number of keys
                                                                                    ###   for every cycle/task pair
          print "    Cycle Keys:  ", cycle_keys.inspect, "\n\n"
  
          puts "!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!"
          puts "DB CYCLES:  #{db_cycle_times.length}"
          db_cycle_times.each do |db_time|
            puts db_time.inspect
          end

          # get list of cycles from XML file
          puts "!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!"
          puts "XML CYCLES"
          cycledefs = workflowdoc.cycledefs
          xml_cycle_times = []
          reftime=cycledefs.collect { |cdef| cdef.next(Time.gm(1900,1,1,0,0)) }.compact.min
          while true do
            break if reftime.nil?
            puts reftime.inspect
            xml_cycle_times << reftime
            reftime=cycledefs.collect { |cdef| cdef.next(reftime+60) }.compact.min
          end

          # find cycle times in XML file that aren't in database
          xml_only_times = (xml_cycle_times - db_cycle_times)
          xml_only_times.sort!
          puts "!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!"
          puts "NUM_DB_CYCLES:  #{db_cycle_times.length}"
          puts "DB_CYCLES:  #{db_cycle_times.inspect}"
          puts "NUM_XML_CYCLES:  #{xml_cycle_times.length}"
          puts "XML_CYCLES:  #{xml_cycle_times.inspect}"
          puts "NUM_XMLONLY_CYCLES:  #{xml_only_times.length}"
          puts "XMLONLY_CYCLES:  #{xml_only_times.inspect}"

          # gather in tasktable
          #   [:cycle, :state, :taskname, :tries, :exit_status, :nunknowns, :cores, :jobid]
          tasktables = TaskTables.new

          # add cycles from database
#DEBUG          puts "TASK NAME:   #{taskname}"
          all_tasks.each do |taskname, task_value|     ### for each task, the value is the cycle hash for that cycle date
  
            tasktable = TaskTable.new(taskname)
            task_value.each do |cycle_key, cycle_value| 
              tasktable.add_cycle(Cycle.new(cycle_key,cycle_value[:jobid],cycle_value[:state],
                                  cycle_value[:exit_status],cycle_value[:tries]))
            end
            tasktables << tasktable
          end

          # add cycles that exist only in XML file to tasktables
          xml_only_times.each do |cycle_time|
            tasktable = TaskTable.new('-')
            tasktable.add_cycle(Cycle.new(cycle_time,'-', '-', '-', '-'))
            tasktables << tasktable
          end


          # print tasktable
          tasktables.print(@options.tasks,@options.cycles,@options.taskfirst)
            
  
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
  
          tasks = workflowdoc.tasks

          # 
          puts tasks.class
          puts tasks.length
          puts tasks[0].class
          puts "************"
          
          ### attributes
  #        tasks.each do |task|
  #          puts "++++++++++++"
  #          puts "Attributes:  #{task.attributes.keys.inspect}"
  #          task.attributes.each do |attr_key, attr_val|
  #            if attr_val.class == CompoundTimeString then 
  #              puts "  #{attr_key}   #{attr_val.to_s(Time.now)}"
  #            else
  #              puts "  #{attr_key}   #{attr_val}"
  #            end
  #          end
  #        puts "************"
  #        end
          
          ### dependencies
  #        tasks.each do |task|
  #          puts "++++++++++++"
  #          puts "Dependency:  #{task.dependency.inspect}"
  #          puts task.dependency.class
  #          puts "************"
  #        end
  
          ### environment variables
  #        tasks.each do |task|
  #          puts "++++++++++++"
  #          puts "Dependency:  #{task.envars.inspect}"
  #          puts task.envars.class
  #          puts "************"
  #        end
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

  end  # Class WorkflowDbStatus

end  # Module WorkflowMgr
