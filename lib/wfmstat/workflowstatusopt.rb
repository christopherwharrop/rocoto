##########################################
#
# Module WFMStat
#
##########################################
module WFMStat

  ##########################################  
  #
  # Class WFMStatOption
  #
  ##########################################
  ### to call:  ./workflowstatusopt.rb -x xmlfile -d dbfile [-c "c1, c2, c3"] [-t "tk1, tk2, tk3"] [-s]
  ###

  class WFMStatOption

    require 'optparse'
    require 'pp'                      
    require 'parsedate'
    
    attr_reader :database, :workflowdoc, :cycles, :tasks, :summary, :taskfirst

    ##########################################  
    #
    # Initialize
    #
    ##########################################
    def initialize(args)

      @database=nil
      @workflowdoc=nil
      @cycles=''
      @tasks=[]
      @summary='false'
      @taskfirst='false'
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
        opts.banner = "Usage:  wfmstat -d database_file -w workflow_document [-c cycle_list] [-t task_list] [-s]"

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
          @cycles=clist
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
          exit
        end

        # Handle option for version
        opts.on("--version","Show Workflow Manager version") do
          puts "Workflow Manager Version #{WorkflowMgr::VERSION}"
          exit
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

      ###  
     
    end  # parse

  end  # Class WFMStatOption

end  # Module WFMStat
