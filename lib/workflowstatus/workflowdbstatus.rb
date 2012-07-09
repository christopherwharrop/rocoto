##########################################
#
# Module WorkflowMgr
#
##########################################
module WorkflowMgr

  ##############################################
  #
  # Class Cycle  -  reopen class to add methods
  #
  ##############################################
  class Cycle

    # sets activated time based on state
    def activated_time_string(fmt="%b %d %Y %H:%M:%S")

      case @state
        when :inactive
          activated="-"
        when :active
          activated=@activated.strftime(fmt)
        when :done 
          activated=@activated.strftime(fmt)
        when :expired
          activated=@activated.strftime(fmt)
      end
      activated
    end

    # sets deactivated time based on state
    def deactivated_time_string(fmt="%b %d %Y %H:%M:%S")

      case @state
        when :inactive
          deactivated="-"
        when :active
          deactivated="-"
        when :done 
          deactivated=@done.strftime(fmt)
        when :expired
          deactivated=@expired.strftime(fmt)
      end
      deactivated
    end
  end

  ##########################################
  #
  # Class SummaryTable
  #
  ##########################################
  class SummaryTable

    def initialize(array_cycles)
      @summary_table = array_cycles
    end

    def print(cycles_arglist_string)

      # print header
      printf "%13s %10s %26s %24s\n","CYCLE".center(12),"STATE".center(8),"ACTIVATED".center(24),
             "DEACTIVATED".center(24)

      # ===============================================
      # if no cycles specified, print all cycles
      # ===============================================
      if cycles_arglist_string.empty? then

        # print cycle date/times
        @summary_table.each do |cycle| 
          printf "%12s %10s %24s %24s\n","#{cycle.cycle.strftime("%Y%m%d%H%M")}", 
                                         "#{cycle.state}",
                                         "#{cycle.activated_time_string}",
                                         "#{cycle.deactivated_time_string}"
        end

      else

        # ===============================================
        # range of cycles
        # ===============================================
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
          @summary_table.each_with_index do |cycle,i|
            index= nil
            index = i if cycle.cycle.between?(cycles_range[0],cycles_range[1])
            if (!index.nil?) then
              printf "%12s %10s %24s %24s\n","#{cycle.cycle.strftime("%Y%m%d%H%M")}", 
                                             "#{cycle.state}",
                                             "#{cycle.activated_time_string}",
                                             "#{cycle.deactivated_time_string}"
            end  # if not nil
          end  # @summary_table do

        # ===============================================
        # list of cycles
        # ===============================================
        else
          cycles_arglist = []
          cycles_arglist_string.split(',').each do |cyclestr|
            parsed_date = ParseDate.parsedate(cyclestr.strip)
            tm = Time.utc(parsed_date[0], parsed_date[1], parsed_date[2], parsed_date[3],
                          parsed_date[4])
            cycles_arglist << tm
          end
          cycles = cycles_arglist.sort
  
          # print out info for specified cycles if cycle matches input_cycle
          @summary_table.each_with_index do |cycle,i|
            index= nil
            index = i if cycles.include?(cycle.cycle)
            if (!index.nil?) then
              printf "%12s %10s %24s %24s\n","#{cycle.cycle.strftime("%Y%m%d%H%M")}", 
                                             "#{cycle.state}",
                                             "#{cycle.activated_time_string}",
                                             "#{cycle.deactivated_time_string}"
            end  # if not nil
          end  # @summary_table do 

        end  # if range or list
      end  # cycle_summary
    end

  end  # SummaryTable

  ##########################################
  #
  # Class JobTables
  #
  ##########################################
  class JobTables

    attr_reader :jobtables

    def initialize
      @jobtables = []
    end

    def << (jobtable)
      @jobtables << jobtable
    end

    # sort Job objects in place by task name
    def sort!
      @jobtables.sort!
    end

    # print table with tasknames sorted 
    #    if no taskname argument list, print out all tasks
    #    if specified taskname does not exist, print nothing
    def print(tasknames_arglist, cycles_arglist_string, taskfirst)

      Job.sort_order([:taskname,:time]) if taskfirst == true
      sort!

      ## print header line
      if taskfirst == true then
        header_format = "%18s %22s %10s %16s %12s %6s\n"
        header_string = "TASK".center(18),"CYCLE".center(18),"JOBID".center(10), 
                        "STATE".center(16),"EXIT STATUS".center(12),"TRIES".center(6)
      else
        header_format = "%22s %23s %10s %16s %12s %6s\n"
        header_string = "CYCLE".center(18),"TASK".center(23),"JOBID".center(10), 
                        "STATE".center(16),"EXIT STATUS".center(12),"TRIES".center(6)
      end
      header = header_format % header_string
      puts header

      ## print out info, if task matches input_taskname
      if tasknames_arglist.empty? then
        print_cycles(cycles_arglist_string,taskfirst,@jobtables)
      else
        newjobtables = []
        tasknames_arglist.sort.each do |input_taskname|
          @jobtables.each_with_index do |jobtable,i|
            newjobtables << @jobtables[i] if input_taskname == jobtable.taskname 
          end
        end  # tasknames_arglist do
        newjobtables.sort!
        print_cycles(cycles_arglist_string,taskfirst,newjobtables)
      end  # if
    end  # def print

    # print cycles for each taskname
    #    if no cycle argument list, print out latest cycle activated
    def print_cycles(cycles_arglist_string,taskfirst,jobtables)

      # ===============================================
      # range of cycles
      # ===============================================
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
        jobtables.each_with_index do |jt,i|
##DEBUG          puts jt.time, jt.taskname
          index= nil
          index = i if jt.time.between?(cycles_range[0],cycles_range[1])
          if (!index.nil?) then
            if taskfirst == true then
              cycle_string = sprintf("%18s %18s", "  #{jt.taskname.ljust(18)}", 
                                     "#{jt.time.strftime("%b %d %Y %H:%M").center(18)}")
            else
              cycle_string = sprintf("%23s %18s","  #{jt.time.strftime("%b %d %Y %H:%M").ljust(23)}", 
                                     "#{jt.taskname.ljust(18)}")
            end
            info_string  =  sprintf("%11s %16s %9s %8s", "#{jt.jobid.to_s[0,10]}", 
                                   "#{jt.state.rjust(16)}", "#{jt.exit_status.to_s[0,5]}", 
                                   "#{jt.tries.to_s[0,6]}")
            puts cycle_string + info_string
          end

        end  # jobtables do
      # ===============================================
      # list of cycles or last cycle, if none specified
      # ===============================================
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
          times = []
          jobtables.each do |jt|
            times << jt.time
          end
          times.uniq!
          cycles = [] << times.last               ## match last cycle activated
        else
          cycles = cycles_arglist.sort
        end

        # print out info for specified cycles if cycle matches input_cycle
        jobtables.each_with_index do |jt,i|
##DEBUG          puts jt.time, jt.taskname
          index= nil
          index = i if cycles.include?(jt.time) 
          if (!index.nil?) then
            if taskfirst == true then
              cycle_string = sprintf("%18s %18s", "  #{jobtables[index].taskname.ljust(18)}", 
                                     "#{jobtables[index].time.strftime("%b %d %Y %H:%M").center(18)}")
            else
              cycle_string = sprintf("%23s %18s", "  #{jobtables[index].time.strftime("%b %d %Y %H:%M").center(23)}", 
                                     "#{jobtables[index].taskname.ljust(18)}")
            end
            info_string  =  sprintf("%11s %16s %9s %8s", "#{jobtables[index].jobid.to_s[0,10]}", 
                                    "#{jobtables[index].state.rjust(16)}", 
                                    "#{jobtables[index].exit_status.to_s[0,5]}", "#{jobtables[index].tries.to_s[0,6]}")
            output_string = cycle_string + info_string
            puts output_string
          end
        end  # jobtables do

        cycles.each do |input_cycle|
          ## check for user specified cycles that do not exist in database or XML file
          table_times = []
          jobtables.each_with_index do |jt,i|
            table_times << jt.time
          end
          common =  [input_cycle] & table_times
          if (common.empty?) then 
            if taskfirst == true then
              cycle_string = sprintf("%18s %23s", "-".center(18), "#{input_cycle.strftime("%b %d %Y %H:%M").center(23)}")
            else
              cycle_string = sprintf("%23s %20s %2s","#{input_cycle.strftime("%b %d %Y %H:%M").center(23)}", "-".center(20), "")
            end
            info_string  =  sprintf("%6s %12s %14s %9s", "-","-".rjust(14),"-","-")
            output_string = cycle_string + info_string
            puts output_string
          end
        end  # cycles do

      end  # if-else

    end  # def print_cycles

  end  # JobTables


  ##########################################
  #
  # Class Job
  #
  ##########################################
  class Job

    include Enumerable

    @@sort_order = [:time, :taskname]

    # pass in an Array of Symbols
    def self.sort_order(neworder)
      @@sort_order = neworder
    end

    attr_reader :taskname, :time, :state, :jobid, :exit_status, :tries

    def initialize(taskname,time,jobid,state,exit_status,tries)
      @taskname = taskname   # String
      @time = time
      @jobid = jobid
      @state = state
      @exit_status = exit_status
      @tries = tries
    end

    # sort by @@sort_order
    def <=>(other)
      # generalize later
      if (@@sort_order.first == :taskname) then
        ret = @taskname <=> other.taskname
        if (@taskname == other.taskname) then
          ret = @time <=> other.time
        end
      else
        ret = @time <=> other.time
        if (@time == other.time) then
          ret = @taskname <=> other.taskname
        end
      end
      ret
    end

  end  # Job

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
    require "workflowmgr/cycle"
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

##DEBUG      puts @options.cycles.inspect
##DEBUG      puts @options.summary.inspect

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
          db_cycle_times << cycle.cycle
        end

        ### ======================
        ###     CYCLE SUMMARY          
        ### ======================
        if @options.summary == true then
  
          summary_table = SummaryTable.new(array_cycles)
          summary_table.print(@options.cycles)
      
        else
  
          # =============JOBS=============
          # get all jobs from database (hash of hash of hashes:  [Task][Cycle][Cycle_hash])
          #    ["ungrib_NAM", "post_nmm_000", "post_nmm_003", "post_nmm_006", "post_nmm_009", 
          #     "real_nmm", "metgrid_nmm", "post_nmm_021", "post_nmm024", "post_nmm_027", 
          #     "wrf_nmm", "post_nmm_042", "post_nmm_045", "post_nmm_048", "post_nmm_012", 
          #     "post_nmm_015", "post_nmm_018", "post_nmm_030", "post_nmm_033", "post_nmm_036", 
          #     "post_nmm_039"]
  
##DEBUG          puts "========================="
##DEBUG          puts "          JOBS          "
##DEBUG          puts "========================="
          all_tasks = @dbServer.get_jobs
          tasknames = all_tasks.keys
          task_db_cycle_times = all_tasks[tasknames.first].keys    ### Task Keys are the cycle dates; Task Values are a hash"
                                                                   ###    assumed all tasks had the same number of cycles
                                                                   ###    not true!!  
                                                                   ###    .first takes the first taskname in hash, which 
                                                                   ###      seems to vary from run to run
          cycle_keys = all_tasks[tasknames.first][task_db_cycle_times.first].keys   ### uses .first since same number of keys
                                                                                    ###   for every cycle/task pair
##DEBUG          print tasknames.first
##DEBUG          print "    Task Keys (#{task_db_cycle_times.length}):  ", task_db_cycle_times.inspect, "\n\n"
##DEBUG          print "Length of hash is:  ", all_tasks.length, "\n\n"
##DEBUG          print "Job Keys:  ", tasknames.inspect, "\n\n"
##DEBUG          print "    Cycle Keys:  ", cycle_keys.inspect, "\n\n"
  
##DEBUG          puts "!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!"
##DEBUG          puts "DB CYCLES:  #{db_cycle_times.length}"
##DEBUG          db_cycle_times.each do |db_time|
##DEBUG            puts db_time.inspect
##DEBUG          end

##DEBUG          puts "!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!"
##DEBUG          puts "XML CYCLES"

          # get list of cycles from XML file
          cycledefs = workflowdoc.cycledefs
          xml_cycle_times = []
          reftime=cycledefs.collect { |cdef| cdef.next(Time.gm(1900,1,1,0,0)) }.compact.min
          while true do
            break if reftime.nil?
##DEBUG            puts reftime.inspect
            xml_cycle_times << reftime
            reftime=cycledefs.collect { |cdef| cdef.next(reftime+60) }.compact.min
          end

          # find cycle times in XML file that aren't in database
          xml_only_times = (xml_cycle_times - db_cycle_times)
          xml_only_times.sort!
##DEBUG          puts "!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!"
##DEBUG          puts "NUM_DB_CYCLES:  #{db_cycle_times.length}"
##DEBUG          puts "DB_CYCLES:  #{db_cycle_times.inspect}"
##DEBUG          puts "NUM_XML_CYCLES:  #{xml_cycle_times.length}"
##DEBUG          puts "XML_CYCLES:  #{xml_cycle_times.inspect}"
##DEBUG          puts "NUM_XMLONLY_CYCLES:  #{xml_only_times.length}"
##DEBUG          puts "XMLONLY_CYCLES:  #{xml_only_times.inspect}"
##DEBUG          puts "!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!"

          # gather in jobtable
          #   [:cycle, :state, :taskname, :tries, :exit_status, :nunknowns, :cores, :jobid]
          jobtables = JobTables.new

          # add cycles from database
          all_tasks.each do |taskname, task_value|     ### for each task, the value is the cycle hash for that cycle date
  
            task_value.each do |cycle_key, cycle_value| 
               jobtables << Job.new(taskname,cycle_key,cycle_value[:jobid],cycle_value[:state],
                                     cycle_value[:exit_status],cycle_value[:tries])
            end
          end

          # add cycles that exist only in XML file to jobtables
          xml_only_times.each do |cycle_time|
##DEBUG            puts cycle_time
            jobtables << Job.new('-',cycle_time,'-', '-', '-', '-')
          end

          # print jobtable
          jobtables.print(@options.tasks,@options.cycles,@options.taskfirst)
            
#######################################################################################################
  
          # open workflow document and get list of tasks from XML file
          #    tasks is an array of Task objects
          #       attributes is a hash
          #           keys:  account, command, maxtries, name, jobname, cores, queue, native, 
          #                  cycledefs, walltime, join, memory
          #       dependency is an Dependency object
          #       envars is 
          #
  
##DEBUG          puts "========================="
##DEBUG          puts "=========TASKS=========="
##DEBUG          puts "========================="
  
          tasks = workflowdoc.tasks

##DEBUG        puts tasks.class
##DEBUG        puts tasks.length
##DEBUG        puts tasks[0].class
##DEBUG        puts "************"
##DEBUG        
          ### attributes
##DEBUG        tasks.each do |task|
##DEBUG          puts "++++++++++++"
##DEBUG          puts "Attributes:  #{task.attributes.keys.inspect}"
##DEBUG          task.attributes.each do |attr_key, attr_val|
##DEBUG            if attr_val.class == CompoundTimeString then 
##DEBUG              puts "  #{attr_key}   #{attr_val.to_s(Time.now)}"
##DEBUG            else
##DEBUG              puts "  #{attr_key}   #{attr_val}"
##DEBUG            end
##DEBUG          end
##DEBUG        puts "************"
##DEBUG        end
          
          ### dependencies
##DEBUG        tasks.each do |task|
##DEBUG          puts "++++++++++++"
##DEBUG          puts "Dependency:  #{task.dependency.inspect}"
##DEBUG          puts task.dependency.class
##DEBUG          puts "************"
##DEBUG        end
  
          ### environment variables
##DEBUG        tasks.each do |task|
##DEBUG          puts "++++++++++++"
##DEBUG          puts "Dependency:  #{task.envars.inspect}"
##DEBUG          puts task.envars.class
##DEBUG          puts "************"
##DEBUG        end
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
