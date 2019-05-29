##########################################
#
# Module WorkflowMgr
#
##########################################
module WorkflowMgr

  ##########################################
  #
  # Class WorkflowVacuumOption
  #
  ##########################################
  class WorkflowVacuumOption < WorkflowOption

    require 'workflowmgr/workflowoption'

    attr_reader :database, :workflowdoc, :age, :verbose

    ##########################################
    #
    # Initialize
    #
    ##########################################
    def initialize(args)

      @age=nil
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
      opts.banner = "Usage:  rocotovacuum [-h] [-v #] -d database_file -w workflow_document -a age"

      # Age in days of jobs to purge
      opts.on("-a","--age n",Integer,"Delete jobs for cycles that expired or completed more than age days ago") do |age|
        @age=age * 3600 * 24
      end

    end

    ##########################################
    #
    # validate_args
    #
    ##########################################
    def validate_opts(opts,args)

      super(opts,args)

      raise OptionParser::ParseError,"The vacuum age (in days) must be specified." if @age.nil?

    end

  end

end
