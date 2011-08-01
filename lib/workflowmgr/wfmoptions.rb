##########################################
#
# Module WorkflowMgr
#
##########################################
module WorkflowMgr

  ##########################################  
  #
  # Class WFMOptions
  #
  ##########################################
  class WFMOptions

    require 'optparse'
    
    attr_reader :database
    attr_reader :xml

    ##########################################  
    #
    # Initialize
    #
    ##########################################
    def initialize(args)

      @database=nil
      @xml=nil
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
        opts.banner = "Usage:  workflowmgr -d database_file -x xml_file [options]"

        # Handle option for specifying the database file
        opts.on("-dARG","-d=ARG","-d ARG","--database=ARG","--database ARG",String,"Path to database store file") do |db|
          @database=db
        end

        # Handle option for specifying the xml file
        opts.on("-xARG","-x=ARG","-x ARG","--xml=ARG","--xml ARG",String,"Path to XML workflow file") do |xml|
          @xml=xml
        end

        # Handle option for help
        opts.on("-h","--help","Show this message") do
          puts opts
          exit
        end

        begin

          # If no options are specified, turn on the help flag
          args=["-h"] if args.empty?

          # Parse the options
          opts.parse!(args)

          # The -d and -x options are mandatory
          raise OptionParser::ParseError,"A database file must be specified" if @database.nil?
          raise OptionParser::ParseError,"An XML workflow file must be specified" if @xml.nil?
  
        rescue OptionParser::ParseError => e
          STDERR.puts e.message, "\n",opts
          exit(-1)
        end
        
      end

    end  # parse

  end  # Class WFMOptions

end  # Module WorkflowMgr