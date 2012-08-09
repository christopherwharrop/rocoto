##########################################
#
# Module WFMStat
#
##########################################
module WFMStat

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
      
      # date format (YYYYMMDDHHMM)
      #  date_format = "%b %d %Y %H:%M"        #mon dd, YYYY HH:MM
      date_format = "%Y%m%d%H%M"             

      # print header line
      if taskfirst == true then
        header_format = "%11s %20s %14s %16s %16s %6s\n"
        header_string = "TASK".rjust(11),"CYCLE".rjust(20),"JOBID".rjust(14), 
                        "STATE".rjust(16),"EXIT STATUS".rjust(16),"TRIES".rjust(6)
      else
        header_format = "%11s %20s %11s %16s %16s %6s\n"
        header_string = "CYCLE".rjust(11),"TASK".rjust(11),"JOBID".rjust(18), 
                        "STATE".rjust(16),"EXIT STATUS".rjust(16),"TRIES".rjust(6)
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
 
      # date format (YYYYMMDDHHMM)
      date_format = "%Y%m%d%H%M"             

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
              cycle_string = sprintf("%18s %14s", "  #{jt.taskname.ljust(14)}", 
                                     "#{jt.time.strftime(date_format).center(18)}")
            else
              cycle_string = sprintf("%14s %5s %18s","  #{jt.time.strftime(date_format).ljust(14)}", 
                                     "", "#{jt.taskname.ljust(18)}")
            end
            info_string  =  sprintf("%11s %16s %9s %10s", "#{jt.jobid.to_s[0,10]}", 
                                   "#{jt.state.rjust(16)}", "#{jt.exit_status.to_s[0,5]}", 
                                   "#{jt.tries.to_s[0,10]}")
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
              cycle_string = sprintf("%18s %14s", "  #{jobtables[index].taskname.ljust(18)}", 
                                     "#{jobtables[index].time.strftime(date_format).center(14)}")
            else
              cycle_string = sprintf("%14s %5s %18s","  #{jt.time.strftime(date_format).ljust(14)}", 
                                     "", "#{jt.taskname.ljust(18)}")
            end
            info_string  =  sprintf("%12s %16s %9s %10s", "#{jt.jobid.to_s[0,12]}", 
                                   "#{jt.state.rjust(16)}", "#{jt.exit_status.to_s[0,5]}", 
                                   "#{jt.tries.to_s[0,10]}")
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
              cycle_string = sprintf("%18s %2s %14s", "-".center(18), "", "#{input_cycle.strftime(date_format).center(14)}")
            else
              cycle_string = sprintf("%16s %2s %18s","#{input_cycle.strftime(date_format).center(16)}", "", "-".center(20))
            end
            info_string  =  sprintf("%9s %15s %14s %9s", "-","-","-","-")
            output_string = cycle_string + info_string
            puts output_string
          end
        end  # cycles do

      end  # if-else

    end  # def print_cycles

  end  # JobTables

end  # Module WorkflowMgr
