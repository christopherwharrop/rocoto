##########################################
#
# module WorkflowMgr
#
##########################################
module WorkflowMgr

  ##########################################
  #
  # Class WorkflowIO
  #
  ##########################################
  class WorkflowIO

    require 'fileutils'
    require 'libxml-ruby/libxml'


    ##########################################
    #
    # initialize
    #
    ##########################################
    def initialize

    end


    ##########################################
    #
    # parseXMLFile
    #
    ##########################################
    def parseXMLFile(filename)

      document= LibXML::XML::Parser.file(filename,:options => LibXML::XML::Parser::Options::NOENT | LibXML::XML::Parser::Options::HUGE).parse
      return document.to_s

    end


    ##########################################
    #
    # ioreadlines
    #
    ##########################################
    def ioreadlines(filename)

      return IO.readlines(filename,nil)[0]

    end


    ##########################################
    #
    # exists?
    #
    ##########################################
    def exists?(filename)

      return File.exists?(filename)

    end


    ##########################################
    #
    # mtime
    #
    ##########################################
    def mtime(filename)

      return File.mtime(filename)

    end


    ##########################################
    #
    # size
    #
    ##########################################
    def size(filename)

      return File.size(filename)

    end


    ##########################################
    #
    # dirname
    #
    ##########################################
    def dirname(filename)

      return File.dirname(filename)

    end


    ##########################################
    #
    # mkdir_p
    #
    ##########################################
    def mkdir_p(dirname)

      FileUtils.mkdir_p(dirname)

    end


    ##########################################
    #
    # filescan
    #
    ##########################################
    def grep(filename,pattern)
      File.open(filename,'rt') { |f|
        f.each_line do |line|
          if line =~ /#{pattern}/
            return true
          end
        end
      }
    end


    ##########################################
    #
    # log
    #
    ##########################################
    def log(logname,msg)

      host=Socket.gethostname
      logdir=File.dirname(logname)
      FileUtils.mkdir_p(logdir)
      File.open(logname,"a+") { |logfile|
        logfile.puts("#{Time.now} :: #{host} :: #{msg}")
      }

    end


    ##########################################
    #
    # roll_log
    #
    ##########################################
    def roll_log(logname)

      # If the log file exists, roll it
      if File.exists?(logname)

        Dir["#{logname}*"].sort.reverse.each do |f|
          if f=~/#{logname}\.(\d+)$/ then
            ext=$1.to_i
            FileUtils.mv(f,"#{logname}.#{ext+1}")
          else
            FileUtils.mv(f,"#{logname}.0")
          end
        end

      end

    end

  end

end
