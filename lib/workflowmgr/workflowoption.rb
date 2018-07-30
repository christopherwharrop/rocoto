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
      @more_args=parse(args)

    end  # initialize

    ##########################################
    # 
    # each_arg
    # 
    ##########################################
    def each_arg
      # Loops over unparsed arguments, yielding each one.
      @more_args.each { |i|
        yield i
      }
    end

  private

    ##########################################  
    #
    # add_opts
    #
    ##########################################
    def add_opts(opts)
        # Command usage text
        opts.banner = "Usage:  rocotorun [-h] [-v #] -d database_file -w workflow_document"

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
    end


    ##########################################  
    #
    # validate_opts
    #
    ##########################################
    def validate_opts(opts,args)
      # Raises exceptions if the options passed were invalid.  The
      # "args" argument is the arguments remaining after parsing all
      # dash options.
      if !args.empty?
        puts "Unrecognized arguments: #{args.join(' ')}"
      end
      raise OptionParser::ParseError,"Unrecognized options" unless args.empty?
      # The -d and -w options are mandatory
      raise OptionParser::ParseError,"A database file must be specified" if @database.nil?
      raise OptionParser::ParseError,"A workflow definition file must be specified" if @workflowdoc.nil?
    end

    ##########################################  
    #
    # parse
    #
    ##########################################
    def parse(args)
      OptionParser.new do |opts|

        add_opts(opts)

        begin

          # If no options are specified, turn on the help flag
          args=["-h"] if args.empty?

          # Parse the options
          opts.parse!(args)

          # Set verbosity level
          WorkflowMgr.const_set("VERBOSE",@verbose)

          # Set workflow id
          WorkflowMgr.const_set("WORKFLOW_ID",File.basename(@workflowdoc))

          # Print usage information if unknown options were passed
          validate_opts(opts,args)

          # Return the remaining arguments.  Note that args is always empty
          # here in the WorkflowOption class because that class raises
          # an exception in unrecognized_opts unless args is empty.
          return args
        rescue OptionParser::ParseError => e
          STDERR.puts e.message, "\n",opts
          Process.exit(-1)
        end
        
      end

    end  # parse

  end  # Class WorkflowOption

end  # Module WorkflowMgr
