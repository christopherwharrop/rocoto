#########################################
#
# Module WorkflowMgr
#
##########################################
require 'English'
module WorkflowMgr
  require 'workflowmgr/batchsystem'

  ##########################################
  #
  # Class NoBatchSystem
  #
  ##########################################
  class NoBatchSystem < BatchSystem
    require 'command'
    require 'exceptions'

    @@qstat_refresh_rate = 30

    require 'workflowmgr/batchsystem'

    #####################################################
    #
    # initialize
    #
    #####################################################
    def initialize(qstat_refresh_rate = @@qstat_refresh_rate)
      super()

      # Initialize hashes to store qstat output and exit records
      @qstat = {}
      @exit_records = {}

      # Set the qstat refresh rate and availability flag
      @qstat_refresh_rate = qstat_refresh_rate
      @qstat_available = true

      # Initialize the qstat table with current data
      refresh_qstat
    rescue StandardError
      raise "NoBatchSystem object could not be initialized\n\n#{$ERROR_INFO}"
    end

    #####################################################
    #
    # refresh_qstat
    #
    #####################################################
    def refresh_qstat
      # Clear the previous qstat data
      @qstat.clear

      # Reset qstat availability flag
      @qstat_available = true

      # Get the username of this process
      username = Etc.getpwuid(Process.uid).name

      # run qstat to obtain the current status of queued jobs
      output = Command.run("ps -u #{username} -f %id %st")
      if output[1] != 0
        raise output[0]
      else
        @qstat_update_time = Time.now
        output[0].each do |s|
          jobdata = s.strip.split(/\s+/)
          next unless jobdata[0] =~ /^(\w+\.\d+)\.0$/

          @qstat[::Regexp.last_match(1)] = jobdata[1]
        end
      end
    rescue StandardError
      @qstat_available = false
      puts $ERROR_INFO
      nil
    end

    #####################################################
    #
    # get_job_state
    #
    #####################################################
    def get_job_state(jid)
      # run qstat to obtain the job's state
      output = Command.run("ps | grep #{jid}")
      if output[1] != 0
        raise output[0]
      else
        state = "done"
        output[0].each do |s|
          if s =~ /^\s*#{jid}\s+.*$/
            state = "r"
            break
          end
        end
      end

      state
    rescue StandardError
      puts "ERROR: The state of job '#{jid}' could not be determined"
      puts $ERROR_INFO
      "unknown"
    end

    #####################################################
    #
    # get_job_exit_status
    #
    #####################################################
    def get_job_exit_status(jid, max_age = 86_400)
      # Set the No_ROOT
      ENV['No_ROOT'] = @sge_root

      # Get the accounting record for the job
      record = find_job(jid, max_age)
      if record.nil?
        exit_status = nil
      else
        exit_status = record.split(":")[12].to_i
        if exit_status == 0
          exit_status = record.split(":")[11].to_i
        end
      end
      exit_status
    rescue StandardError
      puts "Exit status for job #{jid} could not be found\n\n#{$ERROR_INFO}"
      nil
    end

    #####################################################
    #
    # find_job
    #
    #####################################################
    def find_job(jid, max_age = 86_400)
      # Set the SGE_ROOT
      ENV['SGE_ROOT'] = @sge_root

      # Get the current SGE server's hostname
      output = Command.run("#{@sge_path}/qconf -sss")
      if output[1] != 0
        raise output[0]
      end

      server = output[0].split(".")[0].chomp

      # Get our hostname
      host = `hostname -s`.chomp

      # If we are not on the server, invoke this method through an ssh tunnel to the server
      if server != host
        cmd = "ssh -o StrictHostKeyChecking=no #{server} /usr/bin/ruby -r #{__FILE__} " \
        "-e \\''puts SGEBatchSystem.new(\"#{@sge_root}\").find_job(#{jid},#{max_age})'\\'"
        output = Command.run(cmd)
        raise output[0] if output[1] != 0

        record = output[0].chomp
        return record == "nil" ? nil : record
      end

      min_end_time = Time.now - max_age

      # Look for the job record
      record = nil
      error = ""
      catch(:done) do
        2.times do
          # Get a list of accounting files sorted by modification time in reverse order
          files = Dir["#{@acct_path}/accounting*"].sort! do |a, b|
            File.stat(b).mtime <=> File.stat(a).mtime
          end

          # Loop over files reading each one backwards
          count = 0
          files.each do |file|
            fd = if file =~ /\.gz$/
                   IO.popen("gunzip -c #{file} | tac 2>&1")
                 else
                   IO.popen("tac #{file} 2>&1")
                 end
            fd.each do |line|
              error = line
              fields = line.split(/:/)

              # Quit if we've reached the minimum end_time
              end_time = Time.at(fields[10].to_i)
              count = if end_time > Time.at(0) && end_time < Time.at(min_end_time)
                        count + 1
                      else
                        0
                      end
              if count > 10
                fd.close
                throw :done
              end

              # If the jid field matches, return the record
              next unless fields[5] =~ /^#{jid}$/

              record = line
              fd.close
              throw :done
            end
            fd.close unless fd.closed?
            if $CHILD_STATUS != 0
              if error =~ /No such file or directory/
                files = Dir["#{@acct_path}/accounting*"].sort! do |a, b|
                  File.stat(b).mtime <=> File.stat(a).mtime
                end
                retry
              else
                raise error
              end
            end
          end

          sleep 1
        end
      end

      record
    rescue StandardError
      puts "Accounting record for job #{jid} could not be found\n\n#{$ERROR_INFO}"
      nil
    end

    #####################################################
    #
    # find_all_jobs
    #
    #####################################################
    def find_all_jobs(stime, etime, accounts, users, job_pattern, *attrs)
      # Get a list of accounting files sorted by modification time in reverse order
      files = Dir["#{@acct_path}/accounting*"].sort! do |a, b|
        File.stat(b).mtime <=> File.stat(a).mtime
      end

      # Get date strings for the times
      start_str = stime.strftime("%Y%m%d")
      end_str = etime.strftime("%Y%m%d")

      # Get the index of the first file to check
      sindex = files.length - 1
      1.upto(files.length - 1) do |index|
        if files[index].split(".")[1] < start_str
          sindex = index
          break
        end
      end

      # Get the index of the last file to check
      eindex = files.length - 1
      1.upto(files.length - 1) do |index|
        if files[index].split(".")[1] <= end_str
          eindex = index
          break
        end
      end
      if eindex == 1
        eindex = 0
      end

      jobs = {}
      sindex.downto(eindex) do |index|
        file = files[index]
        fd = if file =~ /\.gz$/
               IO.popen("gunzip -c #{file} | cat 2>&1")
             else
               IO.popen("cat #{file} 2>&1")
             end

        until fd.eof?
          record = fd.gets
          if record =~ /^#/
            next
          end

          fields = record.split(":")
          next unless (stime.nil? || fields[10].to_i >= stime.to_i) &&
                      (etime.nil? || fields[10].to_i <= etime.to_i) &&
                      (accounts.nil? || !accounts.index(fields[6]).nil?) &&
                      (users.nil? || !users.index(fields[3]).nil?) &&
                      (job_pattern.nil? || job_pattern.match(fields[4]))

          job_key = "#{fields[5]}_#{fields[8]}"
          if attrs.empty?
            jobs[job_key] = record
          else
            small_record = []
            attrs.each do |attr|
              small_record.push(fields[attr.to_i])
            end
            jobs[job_key] = small_record
          end
        end
        fd.close
      end

      jobs
    end

    #####################################################
    #
    # wait_job_start
    #
    #####################################################
    def wait_job_start(jid, timeout, interval, verbose)
      # Set the SGE_ROOT
      ENV['SGE_ROOT'] = @sge_root

      # Calculate the expiration time
      expire_time = Time.now + timeout

      # Poll the job's state until it starts or the timeout expires
      state = get_job_state(jid)
      if verbose
        puts "#{Time.now} :: Job #{jid} is in state '#{state}'"
      end
      while state != "r" && state != "done" && Time.now < expire_time
        if expire_time - Time.now > interval
          sleep interval
        else
          sleep(expire_time - Time.now)
        end
        state = get_job_state(jid)
        if verbose
          puts "#{Time.now} :: Job #{jid} is in state '#{state}'"
        end
      end

      # Raise an exception if the timeout expired
      if state != "r" && state != "done"
        if verbose
          puts "#{Time.now} :: Timeout expired!"
        end
        raise TimeoutExpired, "Job #{jid} did not start in time"
      else
        0
      end
    end

    #####################################################
    #
    # wait_job_finish
    #
    #####################################################
    def wait_job_finish(jid, timeout, interval, verbose)
      # Set the SGE_ROOT
      ENV['SGE_ROOT'] = @sge_root

      # Calculate the expiration time
      expire_time = Time.now + timeout

      # Poll the job's state until it is done or the timeout has expired
      state = get_job_state(jid)
      if verbose
        puts "#{Time.now} :: Job #{jid} is in state '#{state}'"
      end
      while state != "done" && Time.now < expire_time
        if expire_time - Time.now > interval
          sleep interval
        else
          sleep(expire_time - Time.now)
        end
        state = get_job_state(jid)
        if verbose
          puts "#{Time.now} :: Job #{jid} is in state '#{state}'"
        end
      end

      # If the timeout has expired, raise an exception
      if state != "done"
        if verbose
          puts "#{Time.now} :: Timeout expired!"
        end
        raise TimeoutExpired, "Job #{jid} did not finish in time"
      else
        0
      end
    end

    #####################################################
    #
    # submit
    #
    #####################################################
    def submit(script, attributes)
      # Issue the submit command
      output = Command.run("& #{script} 2>&1 ")
      if output[1] != 0
        raise output[0].to_s
      end

      # Check for success
      if output[0] =~ /[Yy]our job (\d+) .* has been submitted/
        ::Regexp.last_match(1)
      else
        raise output[0].to_s
      end
    rescue StandardError
      raise $ERROR_INFO
    end

    #####################################################
    #
    # qdel
    #
    #####################################################
    def qdel(jid)
      # Set the SGE_ROOT
      ENV['SGE_ROOT'] = @sge_root

      # Run qdel to delete the job
      output = Command.run("#{@sge_path}/qdel #{jid}")
      if output[1] != 0
        raise output[0]
      end

      0
    rescue StandardError
      puts "ERROR: #{@sge_path}/qdel #{jid} failed"
      puts $ERROR_INFO
      1
    end

    #####################################################
    #
    # info
    #
    #####################################################
    def info
      # Set the SGE_ROOT
      ENV['SGE_ROOT'] = @sge_root

      # Run qdel to delete the job
      output = Command.run("/usr/local/fsl/bin/sgeinfo")
      if output[1] != 0
        raise output[0]
      end

      output[0]
    rescue StandardError
      puts "ERROR: /usr/local/fsl/bin/sgeinfo failed"
      puts $ERROR_INFO
      nil
    end

    #####################################################
    #
    # qstat
    #
    #####################################################
    def qstat
      # Set the SGE_ROOT
      ENV['SGE_ROOT'] = @sge_root

      # Run qdel to delete the job
      output = Command.run("/usr/local/fsl/bin/sgestat")
      if output[1] != 0
        raise output[0]
      end

      output[0]
    rescue StandardError
      puts "ERROR: /usr/local/fsl/bin/sgestat failed"
      puts $ERROR_INFO
      nil
    end

    #####################################################
    #
    # rollover
    #
    #####################################################
    def rollover
      # Set the SGE_ROOT
      ENV['SGE_ROOT'] = @sge_root

      # Get the current SGE server's hostname
      output = Command.run("#{@sge_path}/qconf -sss")
      if output[1] != 0
        raise output[0]
      end

      server = output[0].split(".")[0].chomp

      # Get our hostname
      host = `hostname -s`.chomp

      # Don't rollover unless we are on the current server
      return 1 unless host == server

      # Get a list of accounting files sorted by modification time in reverse order
      files = Dir["#{@acct_path}/accounting*"].sort! do |a, b|
        File.stat(b).mtime <=> File.stat(a).mtime
      end

      # Get the date of the first record in the accounting file
      end_time = -1
      file = File.new(files[0])
      file.each do |line|
        next if line =~ /^#/

        end_time = Time.at(line.split(":")[10].to_i)
        break
      end
      return 0 if end_time == -1

      # Calculate the name of the new accounting file
      date_str = end_time.strftime("%Y%m%d")
      fname = "#{@acct_path}/accounting.#{date_str}"

      # If the file already exists roll it over
      if File.exist?(fname)

        # Find all files associated with that date sorted by modification date
        matching_files = files.find_all do |file|
          file =~ /^#{@acct_path}\/accounting\.#{date_str}/
        end
        oldfiles = matching_files.sort! do |a, b|
          File.stat(a).mtime <=> File.stat(b).mtime
        end

        # Roll them over in reverse order
        oldfiles.each do |file|
          # Get the new extension for the file
          ext = if file =~ /^#{@acct_path}\/accounting\.\d+\.(\d+)$/
                  ::Regexp.last_match(1).to_i + 1
                else
                  0
                end

          # Move the file to its new name
          `mv #{file} #{@acct_path}/accounting.#{date_str}.#{ext}`
        end

      end

      # Move the file to it's new name
      `mv #{@acct_path}/accounting #{fname}`

      # Touch a new accounting file
      `touch #{@acct_path}/accounting`

      # Gzip files older than 1 week
      old_files = files.reject do |file|
        file =~ /\.gz$/ || (Time.now - File.stat(file).mtime < 60 * 60 * 24 * 7)
      end
      old_files.each do |file|
        `/bin/gzip #{file}`
      end

      # Return success
      0
    rescue StandardError
      puts "ERROR: Rollover failed"
      puts $ERROR_INFO
      1
    end
  end
end
