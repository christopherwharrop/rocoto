##########################################
#
# Module WFMStat
#
##########################################
module WFMStat

  ##########################################  
  #
  # Class StatusOption
  # 
  ##########################################
  class WFMStatOption

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
        opts.banner = "Usage:  rocotostat -d database_file -w workflow_document [-c cycle_list] [-t task_list] [-s]"

        # Specify the database file
        opts.on("-d","--database file",String,"Path to workflow database file") do |db|
          @database=db
        end
     
        # Specify the XML file
        opts.on("-w","--workflow PATH",String,"Path to workflow definition file") do |workflowdoc|
          @workflowdoc=workflowdoc
        end

        # Cycles of interest
        #      C   C,C,C  C:C  :C   C:
        #        where C='YYYYMMDDHHMM', C:  >= C, :C  <= C
        opts.on("-c","--cycles 'c1,c2,c3' | 'c1:c2' | ':c' | 'c:' ",String,"List of cycles") do |clist|
          case clist
            when /^\d{12}(,\d{12})*$/
              @cycles=clist.split(",").collect { |c| Time.gm(c[0..3],c[4..5],c[6..7],c[8..9],c[10..11]) }
            when /^(\d{12}):(\d{12})$/
              @cycles=(Time.gm($1[0..3],$1[4..5],$1[6..7],$1[8..9],$1[10..11])..Time.gm($2[0..3],$2[4..5],$2[6..7],$2[8..9],$2[10..11]))
            when /^:(\d{12})$/
              @cycles=(Time.gm(1900,1,1,0,0)..Time.gm($1[0..3],$1[4..5],$1[6..7],$1[8..9],$1[10..11]))
            when /^(\d{12}):$/
              @cycles=(Time.gm($1[0..3],$1[4..5],$1[6..7],$1[8..9],$1[10..11])..Time.gm(9999,12,31,23,59))
            else
              puts opts
              Process.exit
          end
        end

        # Tasks of interest
        opts.on("-t","--tasks 'a,b,c'",Array,"List of tasks") do |tasklist|
          @tasks=tasklist
        end
     
        # cycle summary
        opts.on("-s","--summary","Cycle Summary") do 
          @summary=true
        end

        # display by task 
        opts.on("-T","--by_task","Display by Task") do 
          @taskfirst=true
        end

        # Help
        opts.on("-h","--help","Show this message") do
          puts opts
          Process.exit
        end

        # Handle option for version
        opts.on("--version","Show Rocoto version") do
          puts "Rocoto Version #{WorkflowMgr::VERSION}"
          Process.exit
        end

        # Handle option for verbose
        opts.on("-v","--verbose [LEVEL]",/^[0-9]+$/,"Run Rocotostat in verbose mode") do |verbose|
          if verbose.nil?
            @verbose=0
          else
            @verbose=verbose.to_i
          end
        end

        begin

          # If no options are specified, turn on the help flag
          args=["-h"] if args.empty?

          # Parse the options
          opts.parse!(args)

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

  end  # Class StatusOption

end  # Module WFMStat
