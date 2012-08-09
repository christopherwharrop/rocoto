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


    require 'drb'
    require 'workflowmgr/workflowdoc'
    require 'workflowmgr/workflowdb'
    require 'workflowmgr/cycledef'
    require "workflowmgr/cycle"
    require 'workflowmgr/dependency'
    require 'wfmstat/statusoption'
    require 'wfmstat/summarytable'
    require 'wfmstat/jobtables'
    require 'wfmstat/job'

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

##DEBUG      puts @options.cycles.inspect
##DEBUG      puts @options.summary.inspect

      # Get configuration file options
      @config=WorkflowMgr::WorkflowYAMLConfig.new

      # Get the base directory of the WFM installation
      @wfmdir=File.dirname(File.dirname(File.expand_path(File.dirname(__FILE__))))

      # Set up an object to serve the workflow database (but do not open the database)
      @dbServer=WorkflowMgr::DBProxy.new(@options.database,@config)

      # Set up an object to serve file stat info
      @workflowIOServer=WorkflowMgr::WorkflowIOProxy.new(@dbServer,@config)        

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
        workflowdoc = WorkflowMgr::WorkflowXMLDoc.new(@options.workflowdoc, @workflowIOServer)

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

  end  # Class StatusEngine

end  # Module WorkflowMgr
