###########################################
#
# Module WorkflowMgr
#
##########################################
module WorkflowMgr

  require 'workflowmgr/batchsystem'

  ##########################################
  #
  # Class TORQUEBatchSystem
  #
  ##########################################
  class NOBatchSystem < BatchSystem

    require 'etc'
    require 'workflowmgr/utilities'
    require 'fileutils'
    require 'base64'

    #####################################################
    #
    # initialize
    #
    #####################################################
    def initialize(rocoto_pid_dir='/tmp')

      # Initialize an empty hash for job queue records
      @jobqueue={}
      @rocoto_pid_dir=rocoto_pid_dir
    end

    #####################################################
    #
    # statuses
    #
    #####################################################
    def boot_warning
      return 'Booting tasks will run them ON THIS MACHINE, not in the batch system.  You probably do not want to do this.  Do you really want to boot tasks and run them ON THIS MACHINE?'
    end


    #####################################################
    #
    # statuses
    #
    #####################################################
    def statuses(jobids)

      begin
        #WorkflowMgr.stderr("STATUSES?? #{jobids.inspect}",20)

        if jobids.empty?
          #WorkflowMgr.stderr("Empty jobids",20)
        end

        # Initialize statuses to UNAVAILABLE
        jobStatuses={}
        jobids.each do |jobid|
          jobStatuses[jobid] = { :jobid => jobid, :state => "UNAVAILABLE", :native_state => "Unavailable" }
        end

        jobids.each do |jobid|
          jobStatuses[jobid] = self.status(jobid)
        end

      rescue => detail
        WorkflowMgr.stderr("Exception in status: #{detail.to_s}:\nTRACEBACK:\n#{detail.backtrace.join("\n")}",20)
        raise
      ensure
        return jobStatuses
      end

    end


    #####################################################
    #
    # status
    #
    #####################################################
    def status(jobid)
    begin

      #WorkflowMgr.stderr("STATUS #{jobid.inspect} ??",20)

      # Populate the jobs status table if it is empty
      refresh_jobqueue if @jobqueue.empty?
      
      # Return the jobqueue record if there is one
      return @jobqueue[jobid] if @jobqueue.has_key?(jobid)
      
      WorkflowMgr.stderr("STATUS #{jobid} UNKNOWN",20)

      # We didn't find the job, so return an uknown status record
      return { :jobid => jobid, :state => "UNKNOWN", :native_state => "Unknown" }

    rescue => detail
      WorkflowMgr.stderr("Exception in status: #{detail.to_s}:\nTRACEBACK:\n#{detail.backtrace.join("\n")}",20)
      raise
    end  # big begin..rescue for refresh_jobqueue
    end


    #####################################################
    #
    # reap
    #
    #####################################################
    def reap()
      #WorkflowMgr.stderr('in reap')
      begin
      terminal_statuses=['FAILED','SUCCEEDED']

        #WorkflowMgr.stderr("jobqueue is #{@jobqueue.inspect}")

      @jobqueue.each do |jobid,job|
        #WorkflowMgr.stderr("Job #{job} state #{job[:state]} in #{terminal_statuses.inspect}?")
        if terminal_statuses.include? job[:state]
          job_file="#{@rocoto_pid_dir}/#{job[:jobid]}.job"
          kill_file="#{@rocoto_pid_dir}/#{job[:jobid]}.kill"
          [ job_file, kill_file ].each do |file|
            begin
              if File.exists? file
                if Time.new.to_i - File.mtime(file).to_i > 3600
                  WorkflowMgr.stderr("reap job #{jobid}: delete #{file}",10)
                  File.unlink file
                else
                  WorkflowMgr.stderr("reap job #{jobid}: too early to delete #{file}",20)
                end
              end
            rescue IOError, SystemCallError => e
              # Do not terminate when reaping fails.
              WorkflowMgr.stderr("reap job #{jobid}: #{file}: #{e.to_s}",10)
            end
          end
        end
      end
    rescue => detail
      WorkflowMgr.stderr("Exception in reap: #{detail.to_s}:\nTRACEBACK:\n#{detail.backtrace.join("\n")}",20)
      raise
    end  # big begin..rescue for refresh_jobqueue

    end

    #####################################################
    #
    # submit
    #
    #####################################################
    def submit(task)
    begin

      # Initialize the submit command
      cmd=['/usr/bin/env']
      #WorkflowMgr.stderr("(0) CMD SO FAR #{cmd.inspect}",20)

      rocoto_jobid=make_rocoto_jobid
      #WorkflowMgr.stderr("ROCOTO JOBID WILL BE #{rocoto_jobid}")
      #WorkflowMgr.stderr("ROCOTO JOBDIR IS #{@rocoto_pid_dir}")


      process_monitor=File::dirname(__FILE__)+'/../../sbin/rocoto_process_watcher.rb'
      #WorkflowMgr.stderr("process monitor is at #{process_monitor}",20)

      cmd += ["ROCOTO_JOBID=#{rocoto_jobid}",
              "ROCOTO_JOBDIR=#{@rocoto_pid_dir}",
              "ROCOTO_TICKTIME=15"]

      #WorkflowMgr.stderr("(1) CMD SO FAR #{cmd.inspect}",20)

      # Add export commands to pass environment vars to the job
      unless task.envars.empty?
        task.envars.each { |name,env|
          cmd << "#{name}=#{env}"
        }
      end

#      cmd << "rocoto_jobid=#{rocoto_jobid}"

      #WorkflowMgr.stderr("(2) CMD SO FAR #{cmd.inspect}",20)

      # Default values for shell execution bits: no stdout, stdin,
      # stderr, nor any special env vars.
      stdout_file='/dev/null'
      stdin_file='/dev/null'
      stderr_file='/dev/null'
      set_these_vars={}

      # Add Torque batch system options translated from the generic options specification
      task.attributes.each do |option,value|
        case option
          when :stdout
            stdout_file=value
          when :stderr
            stderr_file=value
          when :join
            stdout_file=value
            stderr_file=value
        end
      end

      cmd << 'sh'

      # # <native> are arguments to sh
      # task.each_native do |native_line|
      #     if not native_line.nil? and native_line[0..0]=='-'
      #       cmd << native_line
      #     end
      # end

      cmd << '-c'

      #WorkflowMgr.stderr("(3) CMD SO FAR #{cmd.inspect}",20 )

      [stderr_file, stdout_file].each do |std_file|
        if not File.directory? File.dirname(std_file)
          FileUtils.mkdir_p File.dirname(std_file)
        end
      end

      cmd << '"$@"'

      # # Stdin, stdout, and stderr are handled within sh:
      # if(stdout_file == stderr_file)
      #   cmd << "\"$@\" < #{stdin_file} > #{stdout_file} 2>&1"
      # else
      #   cmd << "\"$@\" < #{stdin_file} 2> #{stderr_file} 1> {stdout_file}"
      # end

      #WorkflowMgr.stderr("(4) CMD SO FAR #{cmd.inspect}",20 )

      # Job name is the process name ($0)
      cmd << "rocoto_bh_#{rocoto_jobid}"

      #WorkflowMgr.stderr("(5) CMD SO FAR #{cmd.inspect}",20 )

      cmd << process_monitor

      # At the end we place the command to run
      cmd << task.attributes[:command]

      WorkflowMgr.stderr("Spawning a daemon process to run #{cmd.inspect}",10)

      result=fork() {
          Process.setsid
          fork() {
            begin
              STDIN.reopen(stdin_file)
              STDERR.reopen(stderr_file,'a')
              if stderr_file == stdout_file
                STDOUT.reopen(STDERR)
              else
                STDOUT.reopen(stdout_file,'a')
              end
              
              WorkflowMgr.stderr("job #{rocoto_jobid}: exec(*#{cmd.inspect})",4)
              
              exec(*cmd)

              WorkflowMgr.stderr("exec failed")
              exit(2)
            rescue => detail
              WorkflowMgr.stderr("Exception in submit: #{detail.to_s}:\nTRACEBACK:\n#{detail.backtrace.join("\n")}",20)
              exit(2)
            end
          }
        }

      #WorkflowMgr.stderr("Back from fork with result=#{result}",4)

      if result.nil? or not result
        WorkflowMgr.stderr("Submission failed: #{result.inspect}",4)
        return nil,''
      else
        #WorkflowMgr.stderr("Submission succeeded for job #{rocoto_jobid.inspect}",4)
        return rocoto_jobid,rocoto_jobid
      end

    rescue => detail
      WorkflowMgr.stderr("Exception in submit: #{detail.to_s}:\nTRACEBACK:\n#{detail.backtrace.join("\n")}",20)
      raise
    end # big begin..rescue for submit
    end # submit

    #####################################################
    #
    # delete
    #
    #####################################################
    def delete(jobid)

      # We ask the job to kill itself:
      kill_file="#{@rocoto_pid_dir}/#{jobid}.kill"
      open(kill_file,'a') do |f|
        WorkflowMgr.stderr("job #{jobid}: write to \"kill file\" #{kill_file}")
        f.puts('@ #{Time.now.to_i} job #{jobid} : kill request from rocoto')
      end

    end

private

    #####################################################
    #
    # make_rocoto_jobid
    #
    #####################################################
    def make_rocoto_jobid()
      # We need a unique jobid.  We'll use a nice, long, string based
      # on the current time in microseconds and some random numbers.
      # This will be a base64 string up to 22 characters in length.

      now_in_usec=(Time.now.tv_sec*1e6 + Time.now.tv_usec).to_i
      big_number=rand(2**64).to_i
      big_number=big_number ^ now_in_usec
      big_hex_number='%015x'%big_number
      result=Base64.encode64(big_hex_number).strip().gsub('=','')
      return result
    end

    #####################################################
    #
    # refresh_jobqueue
    #
    #####################################################
    def refresh_jobqueue
    #WorkflowMgr.stderr("JQ",20)
    begin
      pid_dir=Dir.new(@rocoto_pid_dir)

      pid_dir.each do |filename|
          #WorkflowMgr.stderr("JQ file #{filename}",20)
        if not filename =~ /^(\S+)\.job$/
          #WorkflowMgr.stderr("JQ not a job log file #{filename}",20)
          next # not a job log file
        end
  	record={}

        record[:jobid]=$1
          File.readlines("#{@rocoto_pid_dir}/#{filename}").reverse_each do |line|
          if line=~/HANDLER COMPLETE$/
            record[:native_state]='HANDLER_COMPLETE'
            record[:state]='FAILED' # may be overridden by next step
            next
          elsif line=~/EXIT (\d+)$/
            record[:native_state]='EXIT'
            record[:exit_status]=$1.to_i
            if record[:exit_status] == 0
              record[:state]='SUCCEEDED'
            else
              record[:state]='FAILED'
            end
          elsif line=~/FAIL (.*)$/
            record[:native_state]='CANNOT_START'
            record[:state]='FAILED'
            record[:exit_status]=-1
          elsif line=~/KILL/
            record[:native_state]='KILLING'
            record[:exit_status]=-1
            record[:state]='FAILED'
          elsif line=~/SIGNAL/
            record[:native_state]='SIGNALED'
            record[:exit_status]=-1
            record[:state]='FAILED'
          elsif line=~/START|COMMAND/
            record[:native_state]='STARTING'
            record[:state]='RUNNING'
          elsif line=~/RUNNING/
            record[:native_state]='RUNNING'
            record[:state]='RUNNING'
          else
            WorkflowMgr.stderr("#{filename}: unrecognized line: #{line}",3)
            next
          end
          break
        end

          #WorkflowMgr.stderr("JQ #{filename} is #{record.inspect}",20)
        @jobqueue[record[:jobid]]=record
      end
    rescue => detail
      WorkflowMgr.stderr("Exception in refresh_jobqueue: #{detail.to_s}:\nTRACEBACK:\n#{detail.backtrace.join("\n")}",20)
      raise
    end  # big begin..rescue for refresh_jobqueue
    end  # refresh_jobqueue

  end  # class

end  # module

