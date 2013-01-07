##########################################
#
# Module WFMStat
#
##########################################
module WFMStat

  ##########################################  
  #
  # Class CheckTaskOption
  # 
  ##########################################
  class CheckTaskOption

    require 'optparse'
    require 'pp'                      
    require 'parsedate'
    
    attr_reader :database, :workflowdoc, :cycles, :tasks, :summary, :taskfirst, :verbose

    ##########################################  
    #
    # Initialize
    #
    ##########################################
    def initialize(args)

      @database=nil
      @workflowdoc=nil
      @cycles=nil
      @tasks=nil
      @summary=false
      @taskfirst=false
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
        opts.banner = "Usage:  #{File.basename($0).split(".").first} -d database_file -w workflow_document -c cycle -t task"

        # Specify the database file
        opts.on("-d","--database FILE",String,"Path to workflow database file") do |db|
          @database=db
        end
     
        # Specify the XML file
        opts.on("-w","--workflow PATH",String,"Path to workflow definition file") do |workflowdoc|
          @workflowdoc=workflowdoc
        end

        # Specify the cycle
        opts.on("-c","--cycle CYCLE",String,"Cycle") do |c|
          case c
            when /^\d{12}$/
              @cycles = [Time.gm(c[0..3],c[4..5],c[6..7],c[8..9],c[10..11])]
            else
              puts opts
              Process.exit
          end
        end

        # Tasks of interest
        opts.on("-t","--task TASK",Array,"Task") do |task|
          @tasks=task
        end
     
        # Help
        opts.on("-h","--help","Show this message") do
          puts opts
          Process.exit
        end

        # Handle option for verbose
        opts.on("-v","--verbose [LEVEL]",/^[0-9]+$/,"Run Rocoto in verbose mode") do |verbose|
          if verbose.nil?
            @verbose=0
          else
            @verbose=verbose.to_i
          end
        end

        # Handle option for version
        opts.on("--version","Show Rocoto version") do
          puts "Rocoto Version #{WorkflowMgr.version}"
          Process.exit
        end

        begin

          # If no options are specified, turn on the help flag
          args=["-h"] if args.empty?

          # Parse the options
          opts.parse!(args)

          # Print usage information if unknown options were passed
          raise OptionParser::ParseError,"Unrecognized options" unless args.empty?

          # The -d, -w, -c, and -t options are all mandatory
          raise OptionParser::ParseError,"A database file must be specified" if @database.nil?
          raise OptionParser::ParseError,"A workflow definition file must be specified" if @workflowdoc.nil?
          raise OptionParser::ParseError,"A cycle must be specified" if @cycles.nil?
          raise OptionParser::ParseError,"A task name must be specified" if @tasks.nil?
  
        rescue OptionParser::ParseError => e
          STDERR.puts e.message, "\n",opts
          Process.exit(-1)
        end
        
      end

      ###  
     
    end  # parse

  end  # Class StatusOption

end  # Module WFMStat
