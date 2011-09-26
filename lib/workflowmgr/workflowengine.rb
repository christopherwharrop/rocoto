##########################################
#
# Module WorkflowMgr
#
##########################################
module WorkflowMgr

  ##########################################
  #
  # Class WorkflowEngine
  #
  ##########################################
  class WorkflowEngine

    require 'workflowmgr/workflowconfig'
    require 'workflowmgr/workflowoption'
    require 'workflowmgr/workflowdoc'
    require 'workflowmgr/workflowdb'
    require 'workflowmgr/cycle'


    ##########################################
    #
    # initialize
    #
    ##########################################
    def initialize(args)

      # Get command line options
      @options=WorkflowOption.new(args)

      # Get configuration file options
      @config=WorkflowYAMLConfig.new

    end  # initialize


    ##########################################
    #
    # run
    #
    ##########################################
    def run

      # Initialize the database
      @workflowdb=WorkflowSQLite3DB.new(@options.database)

      # Acquire a lock on the workflow in the database
      @workflowdb.lock_workflow

      begin

        # Initialize the workflow document
        @workflowdoc=WorkflowXMLDoc.new(@options.workflowdoc)

        # Make sure the database contains the current cycle specs
        @workflowdb.update_cyclespecs(@workflowdoc.cycles)

        # Activate new cycles if possible
        activate_new_cycles

        # Get active cycles
        @active_cycles=@workflowdb.get_active_cycles

      ensure

        # Make sure we release the workflow lock in the database
        @workflowdb.unlock_workflow

      end
 
    end  # run

  private

    ##########################################
    #
    # activate_new_cycles
    #
    ##########################################
    def activate_new_cycles

      # Get the cycle specs from the database       
      cyclespecs=@workflowdb.get_cyclespecs.collect do |cyclespec|
        fieldstr=cyclespec[:fieldstr].split(/\s+/)
        if fieldstr.size==3
          CycleInterval.new(cyclespec[:group],fieldstr,cyclespec[:dirty])
        elsif fieldstr.size==6
          CycleCron.new(cyclespec[:group],fieldstr,cyclespec[:dirty])
        else
          raise "ERROR: Unsupported <cycle> type!"
        end
      end

      # Initialize the list of new cycles to activate
      new_cycles=[]

      # Find new cycles to activate
      if @workflowdoc.realtime?

        # For realtime workflows, find the most recent cycle less than or equal to 
        # the current time and activate it if it has not already been activated

        # Get the most recent cycle <= now from cycle specs
        now=Time.now.getgm
        new_cycle=cyclespecs.collect { |c| c.previous(now) }.max

        # Get the latest cycle from the database or initialize it to a very long time ago
	latest_cycle=@workflowdb.get_last_cycle || { :cycle=>Time.gm(1900,1,1,0,0,0) }

        # Activate the new cycle if it hasn't already been activated
        if new_cycle > latest_cycle[:cycle]
          new_cycles << new_cycle
        end

      else

        # For retrospective workflows, find the next N cycles in chronological
        # order that have never been activated.  If the cycle spec has changed,
        # cycles may be added that are older than previously activated cycles.
        # N is the cyclethrottle minus the number of currently active cycles.

        # If any cycle specs are dirty get all cycles from the database.
        # Also get the latest cycle, regardless if any cycle specs are dirty.
        if cyclespecs.any? { |cyclespec| cyclespec.dirty? }

          allcycles=@workflowdb.get_all_cycles.sort { |a,b| a[:cycle] <=> b[:cycle] }

          # Get the latest cycle from the list of cycles or initialize it to a very long time ago
          latest_cycle=allcycles.last || { :cycle=>Time.gm(1900,1,1,0,0,0) }

        else
  
          # Get the latest cycle from the database or initialize it to a very long time ago
	  latest_cycle=@workflowdb.get_last_cycle || { :cycle=>Time.gm(1900,1,1,0,0,0) }

        end

        # Initialize the set of cleaned cycle specs
        cleaned_cyclespecs=[]

        # Get number of active cycles
        nactive_cycles=@workflowdb.get_active_cycles(@workflowdoc.cyclelifespan).size

        # Get the set of new cycles to be added
        (@workflowdoc.cyclethrottle - nactive_cycles).times do

          # Initialize the pool of new cycle candidates
          cyclepool=[]

          # Get the next new cycle for each cycle spec, and add it to the cycle pool
          cyclespecs.each do |cyclespec|

            if cyclespec.dirty?

              # This is a dirty cycle spec so we must start with the first cycle in it
              nexttime=cyclespec.first
              match=allcycles.find { |c| c[:cycle]==nexttime }

              # Get the next cycle until we find a cycle that has not ever been activated
              while !match.nil? do
                nexttime=cyclespec.next(nexttime + 60)
                break if nexttime.nil?
                match=allcycles.find { |c| c[:cycle]==nexttime }           
              end

              # If we found one, add it to the pool of new cycle candidates
              unless nexttime.nil?

                cyclepool << nexttime

                # If the cycle we found is bigger than the latest cycle, mark the cycle spec as clean
                if nexttime >= latest_cycle[:cycle]
                  cleaned_cyclespecs << cyclespec.clean!
                end

              end

            else  # if cyclespec.dirty?

              # This is a clean cycle spec, so get the next cycle > the latest cycle
              nexttime=cyclespec.next(latest_cycle[:cycle] + 60)

              # If we found a cycle, add it to the pool of cycle candidates
              cyclepool << nexttime unless nexttime.nil?

            end  # if cyclespec.dirty?        

          end  # cyclespecs.each

          if cyclepool.empty?

            # If we didn't find any cycles that could be added, stop looking for more
            break

          else

            # Add the earliest cycle in the cycle pool to the list of cycles to activate
            new_cycle=cyclepool.min
            new_cycles << new_cycle

            # Set the latest cycle to the cycle we just added
            if new_cycle > latest_cycle[:cycle]
              latest_cycle={ :cycle=>new_cycle }
            end

            # Add the cycle to the list of all cycles if it exists (at least one cyclespec was dirty when we started)
            allcycles << { :cycle=>new_cycle } unless allcycles.nil?

          end  # if cyclepool.empty?

        end  # .times do

        # Update all the cleaned cycle specs in the database
        unless cleaned_cyclespecs.empty?
          @workflowdb.update_cyclespecs(cleaned_cyclespecs.collect { |spec| { :group=>spec.group, :fieldstr=>spec.fieldstr, :dirty=>0 } } )
        end

      end  # if realtime

      # Add the new cycles to the database
      @workflowdb.add_cycles(new_cycles)

    end  # activate_new_cycles

  end  # Class WorkflowEngine

end  # Module WorkflowMgr
