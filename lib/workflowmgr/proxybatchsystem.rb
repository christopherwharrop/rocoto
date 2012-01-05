##########################################
#
# Module WorkflowMgr
#
##########################################
module WorkflowMgr

  ##########################################
  #
  # Class ProxyBatchSystem
  #
  ##########################################
  class ProxyBatchSystem

    require 'workflowmgr/sgebatchsystem'
  
    ##########################################
    #
    # initialize
    #
    ##########################################
    def initialize(batchSystemClass)

      # Set the batch system being proxied
      @batchsystem=batchSystemClass.new

      # Initialize hashes used to keep track of multithreaded job submission

      # Initialize hash of job submit output
      @status=Hash.new

      # Initialize hash of running threads
      @threads=Hash.new

      # Initialize hash to keep track of which submit outputs we've retrieved
      @harvested=Hash.new

    end


    ##########################################
    #
    # submit
    #
    ##########################################
    def submit(task,cycle)

      # Initialize submission status to NOT harvested
      @harvested[task[:id]]=Hash.new if @harvested[task[:id]].nil?
      @harvested[task[:id]][cycle]=false

      # Create a thread to submit the task
      @threads[task[:id]]=Hash.new if @status[task[:id]].nil?      
      @threads[task[:id]][cycle]=Thread.new {
        @status[task[:id]]=Hash.new if @status[task[:id]].nil?
        @status[task[:id]][cycle]=@batchsystem.submit(task)
      }

    end


    ##########################################
    #
    # get_submit_status
    #
    ##########################################
    def get_submit_status(taskid,cycle)

      # Return nil for jobid and output if the submit thread doesn't exist
      return nil,nil if @threads[taskid].nil?
      return nil,nil if @threads[taskid][cycle].nil?

      # Return nil for jobid and output	if the submit thread is still running
      if @threads[taskid][cycle].alive?
        return nil,nil 
      # Otherwise, get the jobid and output and return it
      else
        status=@status[taskid][cycle]
 
        # Mark this status as harvested
        @harvested[taskid][cycle]=true

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
      @threads.keys.each do |taskid|
        @threads[taskid].keys.each do |cycle|
          return true if @threads[taskid][cycle].alive?
        end
      end
      
      # Check to see if all statuses have been harvested
      @harvested.keys.each do |taskid|
        @harvested[taskid].keys.each do |cycle|
          return true if !@harvested[taskid][cycle]
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
