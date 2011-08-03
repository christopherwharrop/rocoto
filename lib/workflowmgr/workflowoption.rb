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

    ##########################################  
    #
    # Initialize
    #
    ##########################################
    def initialize(args)

      @database=nil
      @workflowdoc=nil
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
        opts.banner = "Usage:  workflowmgr -d database_file -w workflow_document [options]"

        # Handle option for specifying the database file
        opts.on("-dARG","-d=ARG","-d ARG","--database=ARG","--database ARG",String,"Path to database store file") do |db|
          @database=db
        end

        # Handle option for help
        opts.on("-h","--help","Show this message") do
          puts opts
          exit
        end

        # Handle option for version
        opts.on("-v","--version","Show Workflow Manager version") do
          puts "Workflow Manager Version #{WorkflowMgr::VERSION}"
          exit
        end

        # Handle option for specifying the workflow document
        opts.on("-wARG","-w=ARG","-w ARG","--workflow=ARG","--workflow ARG",String,"Path to workflow definition file") do |workflowdoc|
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