##########################################
#
# Module WorkflowMgr
#
##########################################
require 'English'
module WorkflowMgr
  # DRYRUN controls whether workflow operations are executed or just logged.
  # It defaults to 0 (off), but can be overridden via the WORKFLOWMGR_DRYRUN
  # or DRYRUN environment variables, or by explicit assignment in an
  # entrypoint script before this file is loaded.
  DRYRUN = (ENV['WORKFLOWMGR_DRYRUN'] || ENV['DRYRUN'] || '0').to_i unless const_defined?(:DRYRUN)

  #####################################################
  # Helper method to check if we're in dry run mode
  #####################################################
  def self.dryrun_mode?
    DRYRUN.to_i > 0
  end

  require 'timeout'
  require 'open4'

  ##########################################
  #
  # Class WorkflowIOHang
  #
  ##########################################
  class WorkflowIOHang < RuntimeError
  end

  ##########################################
  #
  # Class SchedulerDown
  #
  ##########################################
  class SchedulerDown < RuntimeError
  end

  ##########################################
  #
  # WorkflowMgr.version
  #
  ##########################################
  def self.version
    IO.readlines("#{File.expand_path('../..', __dir__)}/VERSION", nil)[0].strip
  end

  ##########################################
  #
  # WorkflowMgr.timeout
  #
  ##########################################
  def self.timeout(seconds, &block)
    Timeout.timeout(seconds, &block)
  end

  ##########################################
  #
  # WorkflowMgr.ddhhmmss_to_seconds
  #
  ##########################################
  def self.ddhhmmss_to_seconds(ddhhmmss)
    secs = 0
    unless ddhhmmss.nil?
      sign = ddhhmmss[/^-/].nil? ? 1 : -1
      ddhhmmss.split(":").reverse.each_with_index do |i, index|
        if index == 3
          secs += i.to_i.abs * 3600 * 24
        elsif index < 3
          secs += i.to_i.abs * 60**index
        else
          raise "Invalid dd:hh:mm:ss, '#{ddhhmmss}'"
        end
      end
      secs *= sign
    end
    secs
  end

  ##########################################
  #
  # WorkflowMgr.seconds_to_hhmmss
  #
  ##########################################
  def self.seconds_to_hhmmss(seconds)
    s = seconds
    hours = (s / 3600.0).floor
    s -= hours * 3600
    minutes = (s / 60.0).floor
    s -= minutes * 60
    seconds = s

    format("%<hours>0d:%<minutes>02d:%<seconds>02d", hours: hours, minutes: minutes, seconds: seconds)
  end

  ##########################################
  #
  # WorkflowMgr.seconds_to_hhmm
  #
  ##########################################
  def self.seconds_to_hhmm(seconds)
    s = seconds
    hours = (s / 3600.0).floor
    s -= hours * 3600
    minutes = (s / 60.0).ceil
    if minutes > 59
      hours += 1
      minutes = 0
    end

    format("%<hours>0d:%<minutes>02d", hours: hours, minutes: minutes)
  end

  ##########################################
  #
  # WorkflowMgr.config_set
  #
  ##########################################
  def self.config_set(config)
    WorkflowMgr.const_set("CONFIG", config)
  end

  ##########################################
  #
  # WorkflowMgr.options_set
  #
  ##########################################
  def self.options_set(options)
    WorkflowMgr.const_set("OPTIONS", options)
  end

  ##########################################
  #
  # WorkflowMgr.stderr
  #
  ##########################################
  def self.stderr(message, level = 0)
    return if message.nil?
    return if message.empty?

    if level <= VERBOSE
      warn "#{Time.now.strftime('%x %X %Z')} :: #{WORKFLOW_ID} :: #{message}"
    end
  end

  ##########################################
  #
  # WorkflowMgr.log
  #
  ##########################################
  def self.log(message)
    return if message.nil?
    return if message.empty?

    # Name of the current log file
    rocotolog = "#{ENV['HOME']}/.rocoto/#{WorkflowMgr.version}/#{WorkflowMgr::WORKFLOW_ID.sub(/.xml$/, '')}/log"

    # Logging requires exclusive access to the logs
    # Open the log lock file
    File.open("#{rocotolog}.lock", "w") do |lockfile|
      # Try up to three times to acquire an exclusive write lock for the lock file
      got_lock = false
      5.times do
        got_lock = lockfile.flock(File::LOCK_EX | File::LOCK_NB)
        if got_lock
          break
        end

        sleep rand
      end

      # If we get the lock, proceed with logging and rotation if needed
      if got_lock

        begin
          # Only rotate logs if they exist
          if File.exist?(rocotolog)

            # Determine if it is time to rotate the logs
            log_mod_time = File.mtime(rocotolog)
            rotate = log_mod_time.day != Time.now.day

            if rotate

              # Rotate log
              FileUtils.mv(rocotolog, rocotolog + ".#{log_mod_time.strftime('%Y%m%d')}")

              # Get the max age (in days) of the log file from the configuration
              # NOTE: This is a hack due to poor design preventing proper access to the configuration object
              rocotorc = "#{ENV['HOME']}/.rocoto/#{WorkflowMgr.version}/rocotorc"
              max_age = YAML.safe_load_file(rocotorc, permitted_classes: [Symbol], symbolize_names: true)[:MaxLogDays]

              # Remove files last modified more than MaxAge days ago
              Dir["#{rocotolog}.[0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9]"].each do |logfile|
                next unless (Time.now - File.mtime(logfile)) > (max_age * 24 * 3600)

                FileUtils.rm_f(logfile)
              end

            end

          end

          # Log the message
          File.open(rocotolog, "a") do |f|
            f.puts "#{Time.now.strftime('%x %X %Z')} :: #{WorkflowMgr::WORKFLOW_ID} :: #{message}"
          end
        ensure
          # Make sure the lock is released
          lockfile.flock(File::LOCK_UN)
        end

      else
        warn "#{Time.now.strftime('%x %X %Z')} :: #{WorkflowMgr::WORKFLOW_ID} :: " \
             "WARNING! Could not acquire lock to write log the following message"
        warn "#{Time.now.strftime('%x %X %Z')} :: #{WorkflowMgr::WORKFLOW_ID} ::          #{message}"
      end
    end
  end

  ##########################################
  #
  # WorkflowMgr.run4
  #
  ##########################################
  def self.run4(command, timeout = 30)
    # Turn off garbage collection to avoid possible seg faults caused by Ruby
    # trying to allocate objects during GC when calling IO.select and IO#read.
    # See: http://tickets.opscode.com/browse/CHEF-2916
    GC.disable

    begin
      pid, stdin, stdout, stderr = Open4.popen4(command)
      stdin.close
    rescue StandardError
      raise "Execution of '#{command}' unsuccessful: #{$ERROR_INFO}"
    end

    error = ""
    output = ""
    exit_status = 0
    begin
      WorkflowMgr.timeout(timeout) do
        output += stdout.gets(nil) until stdout.eof?
        stdout.close

        error += stderr.gets(nil) until stderr.eof?
        stderr.close

        status = Process.waitpid2(pid)
        exit_status = status[1].exitstatus
      end
    rescue Timeout::Error
      Process.kill('KILL', pid)
      stdout.close
      stderr.close
      WorkflowMgr.log("WARNING! The command '#{command}' timed out after #{timeout} seconds.")
      raise Timeout::Error
    end
    [output, error, exit_status]
  end
end
