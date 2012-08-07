##########################################
#
# Module WorkflowMgr
#
##########################################
module WorkflowMgr

  ##########################################  
  #
  # Class ReportOption
  #
  ##########################################
  class ReportOption

    require 'optparse'
    
    attr_reader :database
    attr_reader :workflowdoc
    attr_reader :verbose

    ##########################################  
    #
    # Initialize
    #
    ##########################################
    def initialize(args)

      @database=nil
      @workflowdoc=nil
      @verbose=0
      parse(args)

    end  # initialize

  private

    ##########################################  
    #
    # parse
    #
    ##########################################
    def parse(args)

      OptionParser.new do |opts|

        # Command usage text
        opts.banner = "Usage:  wfmreport -d database_file -w workflow_document [options]"

        # Handle option for specifying the cycles
        opts.on("-c","--cycles [CYCLES]",/\d{12}(,\d{12})* | \d{12}\: | \:\d{12} | \d{12}\:\d{12}/,"Cycles to report") do |cycles|
          @cycles=cycles
        end

        # Handle option for specifying the database file
        opts.on("-d","--database PATH",String,"Path to database store file") do |db|
          @database=db
        end

        # Handle option for help
        opts.on("-h","--help","Show this message") do
          puts opts
          exit
        end

        # Handle option for verbose
        opts.on("-v","--verbose [LEVEL]",/^[0-9]+$/,"Run Workflow Manager in verbose mode") do |verbose|
          if verbose.nil?
            @verbose=1
          else
            @verbose=verbose.to_i
          end
        end

        # Handle option for version
        opts.on("--version","Show Workflow Manager version") do
          puts "Workflow Manager Version #{WorkflowMgr::VERSION}"
          exit
        end

        # Handle option for specifying the workflow document
        opts.on("-w","--workflow PATH",String,"Path to workflow definition file") do |workflowdoc|
          @workflowdoc=workflowdoc
        end

        begin

          # If no options are specified, turn on the help flag
          args=["-h"] if args.empty?

          # Parse the options
          opts.parse!(args)

          # The -d and -w options are mandatory
          raise OptionParser::ParseError,"A database file must be specified" if @database.nil?
          raise OptionParser::ParseError,"A workflow definition file must be specified" if @workflowdoc.nil?
  
        rescue OptionParser::ParseError => e
          STDERR.puts e.message, "\n",opts
          exit(-1)
        end
        
      end

    end  # parse

  end  # Class WorkflowOption

end  # Module WorkflowMgr