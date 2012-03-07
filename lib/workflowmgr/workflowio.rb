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
    def initialize

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




  end

end