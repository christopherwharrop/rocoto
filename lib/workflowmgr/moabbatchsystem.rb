###########################################
#
# Module WorkflowMgr
#
##########################################
require 'English'
module WorkflowMgr
  require 'workflowmgr/batchsystem'

  ##########################################
  #
  # Class MOABBatchSystem
  #
  ##########################################
  class MOABBatchSystem < BatchSystem
    require 'etc'
    require 'parsedate'
    require 'workflowmgr/utilities'
    require 'workflowmgr/torquebatchsystem'

    #####################################################
    #
    # initialize
    #
    #####################################################
    def initialize(config, moab_root = nil, torque_root = nil)
      super()

      # Get timeouts from the configuration
      @showq_timeout = config.JobQueueTimeout
      @showq_c_timeout = config.JobAcctTimeout

      # Initialize an empty hash for job queue records
      @jobqueue = {}

      # Initialize an empty hash for job accounting records
      @jobacct = {}

      # Currently there is no way to specify the amount of time to
      # look back at finished jobs.  MOAB showq will always return
      # all the records it finds the first time.  So, set this to
      # a big value to make sure showq doesn't get run twice.
      @hrsback = 120

      # Assume the scheduler is up
      @schedup = true
    end

    #####################################################
    #
    # statuses
    #
    #####################################################
    def statuses(jobids)
      raise WorkflowMgr::SchedulerDown unless @schedup

      # Initialize statuses to UNAVAILABLE
      job_statuses = {}
      # rubocop:disable Style/CombinableLoops
      jobids.each do |jobid|
        job_statuses[jobid] = { jobid: jobid, state: "UNAVAILABLE", native_state: "Unavailable" }
      end

      jobids.each do |jobid|
        job_statuses[jobid] = status(jobid)
      end
      # rubocop:enable Style/CombinableLoops

      job_statuses
    rescue WorkflowMgr::SchedulerDown
      @schedup = false
      job_statuses
    end

    #####################################################
    #
    # status
    #
    #####################################################
    def status(jobid)
      raise WorkflowMgr::SchedulerDown unless @schedup

      # Populate the jobs status table if it is empty
      refresh_jobqueue if @jobqueue.empty?

      # Return the jobqueue record if there is one
      return @jobqueue[jobid] if @jobqueue.key?(jobid)

      # Populate the job accounting log table if it is empty
      refresh_jobacct if @jobacct.empty?

      # Return the jobacct record if there is one
      return @jobacct[jobid] if @jobacct.key?(jobid)

      # The state is unavailable since Moab doesn't have the state
      { jobid: jobid, state: "UNKNOWN", native_state: "Unknown" }
    rescue WorkflowMgr::SchedulerDown
      @schedup = false
      { jobid: jobid, state: "UNAVAILABLE", native_state: "Unavailable" }
    end

    #####################################################
    #
    # submit
    #
    #####################################################
    def submit(task)
      # Initialize the submit command
      cmd = "msub"
      input = "#! /bin/sh\n"

      # Add Torque batch system options translated from the generic options specification
      task.attributes.each do |option, value|
        if value.is_a?(String) && value.empty?
          WorkflowMgr.stderr("WARNING: <#{option}> has empty content and is ignored", 1)
          next
        end
        case option
        when :account
          input += "#PBS -A #{value}\n"
        when :queue
          input += "#PBS -q #{value}\n"
        when :partition
          input += "#PBS -l partition=#{value}\n"
        when :cores
          # Ignore this attribute if the "nodes" attribute is present
          next unless task.attributes[:nodes].nil?

          input += "#PBS -l procs=#{value}\n"
        when :nodes
          # Replace -l nodes=a:b+c:d+... with a single -l nodes=
          nodes = 0
          value.split('+').each do |nodespec|
            resources = nodespec.split(':ppn=')
            nodes += resources.shift.to_i
          end
          if nodes < 1
            nodes = 1
          end
          input += "#PBS -l nodes=#{nodes}\n"
        when :walltime
          input += "#PBS -l walltime=#{value}\n"
        when :memory
          input += "#PBS -l vmem=#{value}\n"
        when :stdout
          input += "#PBS -o #{value}\n"
        when :stderr
          input += "#PBS -e #{value}\n"
        when :join
          input += "#PBS -j oe -o #{value}\n"
        when :jobname
          input += "#PBS -N #{value}\n"
        end
      end

      task.each_native do |native_line|
        input += "#PBS #{native_line}\n"
      end

      # Add export commands to pass environment vars to the job
      unless task.envars.empty?
        varinput = ''
        task.envars.each do |name, env|
          varinput += "export #{name}='#{env}'\n"
        end
        input += varinput
      end
      input += "set -x\n"

      # Add the command to execute
      input += task.attributes[:command]

      # Generate the execution script that will be submitted
      tf = Tempfile.new('qsub.in')
      tf.write(input)
      tf.flush

      WorkflowMgr.stderr("Submitting #{task.attributes[:name]} using #{cmd} < #{tf.path} with input {{#{input}}}", 4)

      # Run the submit command
      output = if WorkflowMgr.dryrun_mode?
                 "This is a dryrun"
               else
                 `#{cmd} < #{tf.path} 2>&1`.chomp
               end

      # Parse the output of the submit command
      if output =~ /^((\w+\.)*\d+)$/
        [::Regexp.last_match(1), output]
      else
        [nil, output]
      end
    end

    #####################################################
    #
    # delete
    #
    #####################################################
    def delete(jobid)
      `mjobctl -c #{jobid}`
    end


    private

    #####################################################
    #
    # refresh_jobqueue
    #
    #####################################################
    def refresh_jobqueue
      # Get the username of this process
      username = Etc.getpwuid(Process.uid).name

      begin
        # Run qstat to obtain the current status of queued jobs
        errors = ""
        queued_jobs, errors, exit_status = WorkflowMgr.run4("showq --noblock --xml -u #{username}", @showq_timeout)

        # Raise SchedulerDown if the showq failed
        raise WorkflowMgr::SchedulerDown, errors unless exit_status == 0

        # Return if the showq output is empty
        return if queued_jobs.empty?

        # Parse the XML output of showq, building job status records for each job
        # queued_jobs_doc = LibXML::XML::Parser.string(queued_jobs, options: LibXML::XML::Parser::Options::HUGE).parse
        # queued_jobs_doc = Nokogiri::XML(queued_jobs) do |config|
        #   config.huge
        # end
        queued_jobs_doc = Nokogiri::XML(queued_jobs, options: Nokogiri::XML::ParseOptions::HUGE)
        raise WorkflowMgr::SchedulerDown unless queued_jobs_doc.errors.empty?
      rescue Timeout::Error, WorkflowMgr::SchedulerDown
        WorkflowMgr.log($ERROR_INFO.to_s)
        WorkflowMgr.stderr($ERROR_INFO.to_s, 3)
        raise WorkflowMgr::SchedulerDown
      end

      # For each job, find the various attributes and create a job record
      queued_jobs = queued_jobs_doc.find('//job')
      #  queued_jobs.find
      queued_jobs.each do |job|
        # Initialize an empty job record
        record = {}

        # Look at all the attributes for this job and build the record
        # job.children
        job.attributes.each do |jobstat|
          case jobstat.name
          when /JobID/
            record[:jobid] = jobstat.value
          when /State/
            record[:state] = case jobstat.value
                             when /^Idle$/, /^.*Hold$/, /^Deferred$/
                               "QUEUED"
                             when /^Running$/
                               "RUNNING"
                             else
                               "UNKNOWN"
                             end
            record[:native_state] = jobstat.value
          when /JobName/
            record[:jobname] = jobstat.value
          when /User/
            record[:user] = jobstat.value
          when /ReqProcs/
            record[:cores] = jobstat.value.to_i
          when /Class/
            record[:queue] = jobstat.value
          when /SubmissionTime/
            record[:submit_time] = Time.at(jobstat.value.to_i).getgm
          when /StartTime/
            record[:start_time] = Time.at(jobstat.value.to_i).getgm
          when /StartPriority/
            record[:priority] = jobstat.value.to_i
          else
            record[jobstat.name] = jobstat.value
          end
        end
        # Put the job record in the jobqueue
        @jobqueue[record[:jobid]] = record
      end
      nil
    end

    #####################################################
    #
    # refresh_jobacct
    #
    #####################################################
    def refresh_jobacct
      # Get the username of this process
      username = Etc.getpwuid(Process.uid).name

      # Initialize an empty hash of job records
      @jobacct = {}

      begin
        # Run showq to obtain the current status of queued jobs
        errors = ""
        completed_jobs, errors, exit_status = WorkflowMgr.run4("showq -c --noblock --xml -u #{username}",
                                                               @showq_c_timeout)

        # Raise SchedulerDown if the showq failed
        raise WorkflowMgr::SchedulerDown, errors unless exit_status == 0

        # Return if the showq output is empty
        return if completed_jobs.empty?

        # Parse the XML output of showq, building job status records for each job
        # recordxmldoc = LibXML::XML::Parser.string(completed_jobs, options: LibXML::XML::Parser::Options::HUGE).parse
        # recordxmldoc = Nokogiri::XML(completed_jobs) do |config|
        recordxmldoc = Nokogiri::XML(completed_jobs, options: Nokogiri::XML::ParseOptions::HUGE)
        raise WorkflowMgr::SchedulerDown unless recordxmldoc.errors.empty?
      rescue Timeout::Error, WorkflowMgr::SchedulerDown
        WorkflowMgr.log($ERROR_INFO.to_s)
        WorkflowMgr.stderr($ERROR_INFO.to_s, 3)
        raise WorkflowMgr::SchedulerDown
      end

      # For each job, find the various attributes and create a job record
      recordxml = recordxmldoc.find('//job')
      recordxml.each do |job|
        record = {}
        record[:jobid] = job.attributes['JobID']
        record[:native_state] = job.attributes['State']
        record[:jobname] = job.attributes['JobName']
        record[:user] = job.attributes['User']
        record[:cores] = job.attributes['ReqProcs'].to_i
        record[:queue] = job.attributes['Class']
        record[:submit_time] = Time.at(job.attributes['SubmissionTime'].to_i).getgm
        record[:start_time] = Time.at(job.attributes['StartTime'].to_i).getgm
        record[:end_time] = Time.at(job.attributes['CompletionTime'].to_i).getgm
        record[:duration] = job.attributes['AWDuration'].to_i
        record[:priority] = job.attributes['StartPriority'].to_i
        record[:exit_status] = if job.attributes['State'] =~ /^Removed/ || job.attributes['CompletionCode'] =~ /^CNCLD/
                                 255
                               else
                                 job.attributes['CompletionCode'].to_i
                               end
        record[:state] = if record[:exit_status] == 0
                           "SUCCEEDED"
                         else
                           "FAILED"
                         end

        # Add the record if it hasn't already been added
        @jobacct[record[:jobid]] = record unless @jobacct.key?(record[:jobid])
      end

      nil
    end
  end
end
