##########################################
#
# Module WorkflowMgr
#
##########################################
module WorkflowMgr
  require 'workflowmgr/workflowsubset'

  ##########################################
  #
  # Class WorkflowDBSubset
  #
  ##########################################
  class WorkflowDBSubset < WorkflowSubset
    require 'workflowmgr/cycle'
    require 'workflowmgr/task'
    require 'workflowmgr/job'

    ##########################################
    #
    # Initialize
    #
    ##########################################
    def initialize(all_cycles, all_tasks, xml_cycles, db_cycles, undef_cycles, tasks)
      @xml_cycles_array = Array.new xml_cycles
      @db_cycles_array = Array.new db_cycles
      @undef_cycles_array = Array.new undef_cycles

      @xml_cycles_array.sort!
      @xml_cycles_array.uniq!
      @xml_cycles_set = Set.new @xml_cycles_array

      @db_cycles_array.sort!
      @db_cycles_array.uniq!
      @db_cycles_set = Set.new @db_cycles_array

      @undef_cycles_array.sort!
      @undef_cycles_array.uniq!
      @undef_cycles_set = Set.new @undef_cycles_array

      cycles = []
      @db_cycles_array.each { |c| cycles << c.cycle }
      @undef_cycles_array.each { |c| cycles << c.cycle }
      @xml_cycles_array.each { |c| cycles << c.cycle }
      cycles.sort!
      cycles.uniq!

      super(all_cycles, all_tasks, cycles, tasks)
    end

    ##########################################
    #
    # !empty? equivalents
    #
    ##########################################
    def xml_cycles? = !@xml_cycles_array.empty?
    def db_cycles? = !@db_cycles_array.empty?
    def undef_cycles? = !@undef_cycles_array.empty?


    ##########################################
    #
    # iterators
    #
    ##########################################
    def each_xml_cycle(&block) = @xml_cycles_array.each(&block)
    def each_db_cycle(&block) = @db_cycles_array.each(&block)
    def each_undef_cycle(&block) = @undef_cycles_array.each(&block)


    ##########################################
    #
    # collectors
    #
    ##########################################
    def collect_xml_cycles(&block) = @xml_cycles_array.collect(&block)
    def collect_db_cycles(&block) = @db_cycles_array.collect(&block)
    def collect_undef_cycles(&block) = @undef_cycles_array.collect(&block)
  end # class WorkflowDBSubset
end # module WorkflowMgr
