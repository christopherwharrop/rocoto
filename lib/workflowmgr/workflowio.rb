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


    ##########################################
    #
    # initialize
    #
    ##########################################
    def initialize; end

    ##########################################
    #
    # read
    #
    ##########################################
    def read(filename)
      File.read(filename)
    end

    ##########################################
    #
    # ioreadlines
    #
    ##########################################
    def ioreadlines(filename)
      IO.readlines(filename, nil)[0]
    end

    ##########################################
    #
    # exists?
    #
    ##########################################
    def exist?(filename)
      File.exist?(filename)
    end

    ##########################################
    #
    # mtime
    #
    ##########################################
    def mtime(filename)
      File.mtime(filename)
    end

    ##########################################
    #
    # size
    #
    ##########################################
    def size(filename)
      File.size(filename)
    end

    ##########################################
    #
    # dirname
    #
    ##########################################
    def dirname(filename)
      File.dirname(filename)
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
    def grep(filename, pattern)
      File.open(filename, 'rt') do |f|
        f.each_line do |line|
          if line =~ /#{pattern}/
            return true
          end
        end
      end
      false
    end

    ##########################################
    #
    # log
    #
    ##########################################
    def log(logname, msg)
      host = Socket.gethostname
      logdir = File.dirname(logname)
      FileUtils.mkdir_p(logdir)
      File.open(logname, "a+") do |logfile|
        logfile.puts("#{Time.now} :: #{host} :: #{msg}")
      end
    end

    ##########################################
    #
    # roll_log
    #
    ##########################################
    def roll_log(logname)
      # If the log file exists, roll it
      if File.exist?(logname)

        Dir["#{logname}*"].sort.reverse.each do |f|
          if f =~ /#{logname}\.(\d+)$/
            ext = ::Regexp.last_match(1).to_i
            FileUtils.mv(f, "#{logname}.#{ext + 1}")
          else
            FileUtils.mv(f, "#{logname}.0")
          end
        end

      end
    end
  end
end
