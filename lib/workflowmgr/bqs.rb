##########################################
#
# Module WorkflowMgr
#
##########################################
module WorkflowMgr

  ##########################################
  #
  # Class BQS
  #
  ##########################################
  class BQS

    require 'thread/pool'
    require 'workflowmgr/workflowdb'
    require 'workflowmgr/moabbatchsystem'
    require 'workflowmgr/moabtorquebatchsystem'
    require 'workflowmgr/torquebatchsystem'
    require 'workflowmgr/pbsprobatchsystem'
    require 'workflowmgr/lsfbatchsystem'
    require 'workflowmgr/slurmbatchsystem'
    require 'workflowmgr/cobaltbatchsystem'
    require 'workflowmgr/task'

    ##########################################
    #
    # initialize
    #
    ##########################################
    def initialize(batchSystem,dbFile,config)

      Thread::Pool.abort_on_exception = true

      # Set the batch system
      @batchsystem=batchSystem

      # We can't create a thread pool yet because the DRb server hasn't been started yet
      @poolSize=config.SubmitThreads
      @pool=nil

      # Initialize hash of job submit output
      @status=Hash.new

      # Initialize hash of running threads
      @running=Hash.new

      # Initialize hash to keep track of which submit outputs we've retrieved
      @harvested=Hash.new

    end


    ##########################################
    #
    # submit
    #
    ##########################################
    def submit(task,cycle)

      # Initialize a thread pool for multithreaded job submission if we don't have one yet
      @pool = Thread.pool(@poolSize) if @pool.nil?

      # Initialize hashes for this task
      @harvested[task.attributes[:name]] = Hash.new if @harvested[task.attributes[:name]].nil?
      @running[task.attributes[:name]] = Hash.new if @status[task.attributes[:name]].nil?
      @status[task.attributes[:name]] = Hash.new if @status[task.attributes[:name]].nil?

      # Spawn a thread to submit the job
      @pool.process do

        # Initialize submission status to NOT harvested
        @harvested[task.attributes[:name]][cycle.to_i]=false

        # Mark this job submission in progress
        @running[task.attributes[:name]][cycle.to_i] = true

        # Submit the job
        @status[task.attributes[:name]][cycle.to_i]=@batchsystem.submit(task)

        # Mark this job submission as done
        @running[task.attributes[:name]][cycle.to_i] = false

      end

    end


    ##########################################
    #
    # get_submit_status
    #
    ##########################################
    def get_submit_status(taskid,cycle)

      # Return nil for jobid and output if the submit thread doesn't exist
      return nil,nil if @running[taskid].nil?
      return nil,nil if @running[taskid][cycle.to_i].nil?

      # Return nil for jobid and output	if the submit thread is still running
      if @running[taskid][cycle.to_i]
        return nil,nil
      # Otherwise, get the jobid and output and return it
      else
        status=@status[taskid][cycle.to_i]

        # Mark this status as harvested
        @harvested[taskid][cycle.to_i]=true

        # Return the output of the job submission
        return status
      end

    end


    ##########################################
    #
    # running?
    #
    ##########################################
    def running?

      # Check to see if any threads are still running
      @running.keys.each do |taskid|
        @running[taskid].keys.each do |cycle|
          return true if @running[taskid][cycle.to_i]
        end
      end

      # Check to see if all statuses have been harvested
      @harvested.keys.each do |taskid|
        @harvested[taskid].keys.each do |cycle|
          return true if !@harvested[taskid][cycle.to_i]
        end
      end

      return false

    end


    ##########################################
    #
    # method_missing
    #
    ##########################################
    def method_missing(name,*args)

      return @batchsystem.send(name,*args)

    end


  end

end
