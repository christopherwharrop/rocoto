##########################################
#
# Module WorkflowMgr
#
##########################################
module WorkflowMgr
  require 'workflowmgr/workflowselection'
  require 'workflowmgr/workflowdbsubset'
  require 'workflowmgr/selectionutil'

  class WorkflowDBSelection < WorkflowMgr::WorkflowSelection
    ##########################################
    #
    # db_query_cycles
    #
    ##########################################

    def db_query_cycles(db_server, cycledefs)
      # Initialize empty lists of cycles
      dbcycles = []
      xmlcycles = []
      undefcycles = []

      # Get the cycles of interest that are in the database
      if @cycles.nil? || @cycles.empty?
        # Get the latest cycle
        last_cycle = db_server.load_last_cycle
        dbcycles << last_cycle unless last_cycle.nil?
      else
        @cycles.each do |cycopt|
          case cycopt
          when Range
            # Get all cycles within the range
            dbcycles += db_server.load_cycles({ start: cycopt.first, end: cycopt.last })

            # Find every XML cycle in the range
            xml_cycle_times = []
            first_times = cycledefs.collect do |cdef|
              cdef.next(cycopt.first, false)
            end
            reftime = first_times.compact.collect { |c| c[0] }.min
            loop do
              break if reftime.nil?
              break if reftime > cycopt.last

              xml_cycle_times << reftime
              next_times = cycledefs.collect do |cdef|
                cdef.next(reftime + 60, false)
              end
              reftime = next_times.compact.collect { |c| c[0] }.min
            end

            # Add the cycles that are in the XML but not in the DB
            xmlcycles = (xml_cycle_times - dbcycles.collect(&:cycle)).collect { |c| WorkflowMgr::Cycle.new(c) }
          when Time
            cycle = db_server.load_cycles({ start: cycopt, end: cycopt })
            if cycle.empty?
              undefcycles << WorkflowMgr::Cycle.new(cycopt)
            else
              dbcycles += cycle
            end
          when Array
            # Get the specific cycles asked for
            cycopt.each do |c|
              cycle = db_server.load_cycles({ start: c, end: c })
              if cycle.empty?
                undefcycles << WorkflowMgr::Cycle.new(c)
              else
                dbcycles += cycle
              end
            end
          when WorkflowMgr::CycleDefSelection
            these_cycles = []
            cycledefs.each do |cdef|
              next unless cycopt.name == cdef.group

              cdef.each(cdef.first, false) do |cyc|
                these_cycles << cyc
              end
            end
            this_set = Set.new these_cycles
            cyc_first = these_cycles.min
            cyc_last = these_cycles.max
            db_cycles_for_this = db_server.load_cycles(reftime = { start: cyc_first, last: cyc_last })
            db_set = Set.new db_cycles_for_this

            xml_set = this_set - db_set

            dbcycles.concat db_set.to_a

            xml_set.each { |c| xmlcycles << WorkflowMgr::Cycle.new(c) }

          when ALL_POSSIBLE_CYCLES
            dbcycles += db_server.load_cycles
          else
            raise "Invalid cycle specification type=#{cycopt.class.name} value=#{cycopt.inspect}"
          end
        end
      end

      dbcycles.sort!
      dbcycles.uniq!

      xmlcycles.sort!
      xmlcycles.uniq!

      undefcycles.sort!
      undefcycles.uniq!

      [dbcycles, xmlcycles, undefcycles]
    end

    ##########################################
    #
    # make_subset
    #
    ##########################################
    def make_subset(tasks, cycledefs, db_server)
      selected_tasks = select_tasks(tasks)

      db_cycles, xml_cycles, undef_cycles = db_query_cycles(db_server, cycledefs)

      WorkflowDBSubset.new(@all_cycles, @all_tasks, xml_cycles, db_cycles, undef_cycles, selected_tasks)
    end
  end
end
