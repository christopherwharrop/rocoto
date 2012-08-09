##########################################
#
# Module WFMStat
#
##########################################
module WFMStat

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


end  # Module WorkflowMgr
