##########################################
#
# Module WorkflowMgr
#
##########################################
module WorkflowMgr

  ##########################################  
  #
  # Class WorkflowOption
  #
  ##########################################
  class WorkflowOption

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
      @verbose=1
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
        opts.banner = "Usage:  rocotorun -d database_file -w workflow_document [options]"

        # Handle option for specifying the database file
        opts.on("-d","--database PATH",String,"Path to database store file") do |db|
          @database=db
        end

        # Handle option for help
        opts.on("-h","--help","Show this message") do
          puts opts
          Process.exit(0)
        end

        # Handle option for verbose
        opts.on("-v","--verbose [LEVEL]",/^[0-9]+$/,"Run Rocoto in verbose mode") do |verbose|
          if verbose.nil?
            @verbose=1
          else
            @verbose=verbose.to_i
          end
          WorkflowMgr.const_set("VERBOSE",@verbose)
        end

        # Handle option for version
        opts.on("--version","Show Rocoto version") do
          puts "Rocoto Version #{WorkflowMgr.version}"
          Process.exit(0)
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

          # Set verbose to 0 if not set by options
          WorkflowMgr.const_set("VERBOSE",0) unless WorkflowMgr.const_defined?("VERBOSE")

          # Print usage information if unknown options were passed
          raise OptionParser::ParseError,"Unrecognized options" unless args.empty?

          # The -d and -w options are mandatory
          raise OptionParser::ParseError,"A database file must be specified" if @database.nil?
          raise OptionParser::ParseError,"A workflow definition file must be specified" if @workflowdoc.nil?
  
        rescue OptionParser::ParseError => e
          STDERR.puts e.message, "\n",opts
          Process.exit(-1)
        end
        
      end

    end  # parse

  end  # Class WorkflowOption

end  # Module WorkflowMgr
