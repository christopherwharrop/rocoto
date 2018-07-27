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

    def db_query_cycles(dbServer,cycledefs)
      # Initialize empty lists of cycles
      dbcycles=[]
      xmlcycles=[]
      undefcycles=[]

      # Get the cycles of interest that are in the database
      if @cycles.nil? or @cycles.empty?
        # Get the latest cycle
        last_cycle=dbServer.get_last_cycle
        dbcycles << last_cycle unless last_cycle.nil?
      else
        @cycles.each do |cycopt|
          if cycopt.is_a?(Range)
            # Get all cycles within the range
            dbcycles += dbServer.get_cycles( {:start=>cycopt.first, :end=>cycopt.last } )

            # Find every XML cycle in the range
            xml_cycle_times = []
            reftime=cycledefs.collect { |cdef| cdef.next(cycopt.first,by_activation_time=false) }.compact.collect {|c| c[0] }.min
            while true do
              break if reftime.nil?
              break if reftime > cycopt.last
              xml_cycle_times << reftime
              reftime=cycledefs.collect { |cdef| cdef.next(reftime+60,by_activation_time=false) }.compact.collect {|c| c[0] }.min
            end
            
            # Add the cycles that are in the XML but not in the DB
            xmlcycles = (xml_cycle_times - dbcycles.collect { |c| c.cycle } ).collect { |c| WorkflowMgr::Cycle.new(c) }
            
          elsif cycopt.is_a?(Array)
            # Get the specific cycles asked for
            cycopt.each do |c|
              cycle = dbServer.get_cycles( {:start=>c, :end=>c } )
              if cycle.empty?
                undefcycles << WorkflowMgr::Cycle.new(c)
              else
                dbcycles += cycle
              end
            end
          elsif cycopt.is_a? WorkflowMgr::CycleDefSelection
            these_cycles=[]
            cycledefs.each do |cdef|
              if cycopt.name == cdef.group
              cdef.each(cdef.first,by_activation_time=false) do |cyc|
                  these_cycles << cyc
                end
              end
            end
            this_set=Set.new these_cycles
            cyc_first=these_cycles.min
            cyc_last=these_cycles.max
            db_cycles_for_this=dbServer.get_cycles(reftime={:start=>cyc_first,:last=>cyc_last})
            db_set=Set.new db_cycles_for_this

            xml_set=this_set-db_set
            
            dbcycles.concat db_set.to_a

            xml_set.each {|c| xmlcycles << WorkflowMgr::Cycle.new(c)}

          elsif cycopt == ALL_POSSIBLE_CYCLES
            dbcycles += dbServer.get_cycles()
          else
            raise "Invalid cycle specification"
          end
        end
      end

      dbcycles.sort!
      dbcycles.uniq!

      xmlcycles.sort!
      xmlcycles.uniq!

      undefcycles.sort!
      undefcycles.uniq!

      return [dbcycles,xmlcycles,undefcycles]
    end


    ##########################################  
    #
    # make_subset
    #
    ##########################################
    def make_subset(tasks,cycledefs,dbServer)
      selected_tasks=select_tasks(tasks)

      db_cycles,xml_cycles,undef_cycles = db_query_cycles(dbServer,cycledefs)

      return WorkflowDBSubset.new(@all_cycles,@all_tasks,xml_cycles,db_cycles,undef_cycles,selected_tasks)
    end
  end
end # module WorkflowMgr
