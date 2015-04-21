##########################################
#
# Module WorkflowMgr
#
##########################################
module WorkflowMgr

  ##########################################  
  #
  # Class WorkflowBootOption
  #
  ##########################################
  class WorkflowBootOption < WorkflowOption

    require 'workflowmgr/workflowoption'

    attr_reader :database, :workflowdoc, :cycles, :tasks, :metatasks, :verbose

    ##########################################  
    #
    # Initialize
    #
    ##########################################
    def initialize(args)

      @cycles=nil
      @tasks=nil
      @metatasks=nil
      super(args)

    end

  private

    ##########################################
    #
    # add_opts
    #
    ##########################################
    def add_opts(opts)

      super(opts)

      # Override the command usage text
      opts.banner = "Usage:  rocotostat [-h] [-v #] -d database_file -w workflow_document [-c cycle_list] [-t task_list] [-m metatask_list]"

      # Cycles of interest
      #      C   C,C,C  C:C  :C   C:
      #        where C='YYYYMMDDHHMM', C:  >= C, :C  <= C 
      opts.on("-c","--cycles 'c1,c2,c3' | 'c1:c2' | ':c' | 'c:' | : | all",String,"List of cycles") do |clist|
        case clist
        when /^\d{12}(,\d{12})*$/
          @cycles=clist.split(",").collect { |c| Time.gm(c[0..3],c[4..5],c[6..7],c[8..9],c[10..11]) }
        when /^(\d{12}):(\d{12})$/
          @cycles=(Time.gm($1[0..3],$1[4..5],$1[6..7],$1[8..9],$1[10..11])..Time.gm($2[0..3],$2[4..5],$2[6..7],$2[8..9],$2[10..11]))
        when /^:(\d{12})$/
          @cycles=(Time.gm(1900,1,1,0,0)..Time.gm($1[0..3],$1[4..5],$1[6..7],$1[8..9],$1[10..11]))
        when /^(\d{12}):$/
          @cycles=(Time.gm($1[0..3],$1[4..5],$1[6..7],$1[8..9],$1[10..11])..Time.gm(9999,12,31,23,59))
        when /^all|:$/i
          @cycles=(Time.gm(1900,1,1,0,0)..Time.gm(9999,12,31,23,59))
        else
          puts opts
          puts "Unrecognized -c option #{clist}"
          Process.exit
        end
      end

      # Tasks of interest
      opts.on("-t","--tasks 'a,b,c'",Array,"List of tasks") do |tasklist|
        @tasks=tasklist
      end

      # Metaasks of interest
      opts.on("-m","--metatasks 'a,b,c'",Array,"List of metatasks") do |metatasklist|
        @metatasks=metatasklist
      end

    end

    ##########################################
    #
    # validate_args
    #
    ##########################################
    def validate_opts(opts,args)

      super(opts,args)

      raise OptionParser::ParseError,"At least one cycle must be specified." if @cycles.nil?

      raise OptionParser::ParseError,"At least one task or metatask must be specified." if (@tasks.nil? && @metatasks.nil?)

    end

  end

end
