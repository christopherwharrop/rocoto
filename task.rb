unless defined? $__task__

if File.symlink?(__FILE__)
  $:.unshift(File.dirname(File.readlink(__FILE__))) unless $:.include?(File.dirname(File.readlink(__FILE__))) 
else
  $:.unshift(File.dirname(__FILE__)) unless $:.include?(File.dirname(__FILE__)) 
end
$:.unshift("#{File.dirname(__FILE__)}/usr/lib64/ruby/site_ruby/1.8/x86_64-linux") 

##########################################
#
# Class Task
#
##########################################
class Task

  require 'job.rb'

  attr_reader :state
  attr_reader :name
 
  #####################################################
  #
  # initialize
  #
  #####################################################
#  def initialize(name,command,scheduler,tries,throttle,properties,environment,dependencies,keepalive,log=nil)
  def initialize(name,command,scheduler,cyclecrons,tries,throttle,properties,environment,dependencies,hangdependencies,deadlinedependencies,log=nil)

    @name=name
    @command=command
    @scheduler=scheduler
    @cyclecrons=cyclecrons
    @tries=tries
    @throttle=throttle
    @properties=properties
    @environment=environment
    @dependencies=dependencies
    @hangdependencies=hangdependencies
    @deadlinedependencies=deadlinedependencies
    @log=log
    @expired=Hash.new
    @ntries=Hash.new
    @nthrottle=0
    @jobs=Hash.new
    @nruns=0
    @cumulative_runtime=0

  end


  #####################################################
  #
  # alter
  #
  #####################################################
#  def alter(name,command,scheduler,tries,throttle,properties,environment,dependencies,keepalive,log=nil)
  def alter(name,command,scheduler,cyclecrons,tries,throttle,properties,environment,dependencies,hangdependencies,deadlinedependencies,log=nil)

    @name=name
    @command=command
    @scheduler=scheduler
    @cyclecrons=cyclecrons
    @tries=tries
    @throttle=throttle
    @properties=properties
    @environment=environment
    @dependencies=dependencies
    @hangdependencies=hangdependencies
    @deadlinedependencies=deadlinedependencies
    @log=log

  end


  #####################################################
  #
  # running?
  #
  #####################################################
  def running?(cycle)

    return false if (@jobs[cycle].nil? || @jobs[cycle].id.nil?)
    return !done?(cycle)
 
  end


  #####################################################
  #
  # done?
  #
  #####################################################
  def done?(cycle)

    return done_okay?(cycle) || crashed?(cycle)
 
  end


  #####################################################
  #
  # done_okay?
  #
  #####################################################
  def done_okay?(cycle)

    return false if @jobs[cycle].nil?
    return @jobs[cycle].done_okay?
 
  end


  #####################################################
  #
  # crashed?
  #
  #####################################################
  def crashed?(cycle)

    return false if @jobs[cycle].nil?
    return (@jobs[cycle].crashed? && @tries.nonzero? && @ntries[cycle] >= @tries)
 
  end

  #####################################################
  #
  # expired?
  #
  #####################################################
  def expired?(cycle)

    @expired=Hash.new if @expired.nil? 
    return false if @expired[cycle].nil?
    return @expired[cycle]

  end

  #####################################################
  #
  # tasks_per_hour
  #
  #####################################################
  def tasks_per_hour

    if @nruns < 1
      return nil
    else
      avg_runtime=@cumulative_runtime.to_f/@nruns/3600
      return @throttle/avg_runtime
    end

  end


  #####################################################
  #
  # update_state
  #
  #####################################################
  def update_state(cycle)

    # Update the state of the job
    begin
      @jobs[cycle].update_state
    rescue
      @log.log(cycle,"#{$!}")
      return
    ensure
      @log.log(cycle,"#{@name} job id=#{@jobs[cycle].id} in state '#{@jobs[cycle].state}'")
    end

    # If the job is done (even if it's not done_okay) then decrement the throttle     
    if @jobs[cycle].done?
      @nthrottle-=1
    end

    # If the job is done_okay, then update the mean runtime and nruns
    if @jobs[cycle].done_okay?
      @nruns+=1
      @cumulative_runtime+=@jobs[cycle].execution_time
      @log.log(cycle,"#{@name} job id=#{@jobs[cycle].id} ran for #{@jobs[cycle].execution_time} seconds")
    end

  end

  #####################################################
  #
  # submit
  #
  #####################################################
  def submit(cycle)

    # Set up the task's properties for the current cycle
    properties=Hash.new
    @properties.each { |name,property|
      properties[name]=property.value(cycle)
    }

    # Create a job to carry out this task
    @jobs[cycle]=Job.new(@command,@scheduler,properties)

    # Submit the job
    begin

      # Set up the task's environment for the current cycle
      save_env=Hash.new
      ENV.each_key { |name|
        save_env[name]=ENV[name]
      }
      @environment.each { |name,env|
        ENV[name]=env.value(cycle)
      }

      @jobs[cycle].submit
      @log.log(cycle,"Submitted #{@name} job id=#{@jobs[cycle].id}")
    rescue
      @log.log(cycle,"#{$!}")
      return
    ensure
      if @ntries[cycle].nil?
        @ntries[cycle]=1
      else
        @ntries[cycle]+=1   
      end

      # Restore environment
      ENV.clear
      save_env.each { |var,value|
        ENV[var]=value
      }              

    end

    # Increment the throttle
    @nthrottle+=1

  end

  #####################################################
  #
  # run
  #
  #####################################################
  def run(cycle)

    # Make sure cycle is valid for this task
    return unless @cyclecrons.any? { |cyclecron| cyclecron.has_cycle?(cycle) }
    
    # Don't do anything if the task is already done for this cycle
    return if self.done?(cycle)

    # Don't do anthing if the task is expired
    return if self.expired?(cycle)

    # Determine if the task has expired
    if @deadlinedependencies.nil?
      @expired[cycle]=false
    else
      @expired[cycle]=@deadlinedependencies.resolved?(cycle)
    end

    # Update and check the status of the task's job if it already exists
    unless @jobs[cycle].nil?

      # Update the state of the task for this cycle
      self.update_state(cycle)

      # If the job finished ok, simply return
      return if @jobs[cycle].done_okay?

      # If the job crashed, attempt to resubmit it
      if @jobs[cycle].crashed?

        # Log the crash
        @log.log(cycle,"#{@name} job id=#{@jobs[cycle].id} crashed, exit status=#{@jobs[cycle].exit_status}")
        puts "Cycle #{cycle.strftime("%Y%m%d%H")}:: #{@name} job id=#{@jobs[cycle].id} crashed, exit status=#{@jobs[cycle].exit_status}"

        # Check resubmit counter
        if @tries > 0 && @ntries[cycle] >= @tries
          @log.log(cycle,"#{@name} has been tried #{@ntries[cycle]} times, giving up")
          puts "Cycle #{cycle.strftime("%Y%m%d%H")}:: #{@name} has been tried #{@ntries[cycle]} times, giving up"
        # Check to make sure throttle is not exceeded
        elsif (@nthrottle >= @throttle && @throttle > 0)
          @log.log(cycle,"#{@name} cannot be resubmitted now because the maximum throttle (#{@throttle}) has been reached")
        # Resubmit task if it isn't expired
        elsif !self.expired?(cycle)
          self.submit(cycle)
        end

      end

      # If the job is running, check to see if it has hung
      unless @hangdependencies.nil?
        if @jobs[cycle].running?
          if @hangdependencies.resolved?(cycle)

            # Log the hang
            @log.log(cycle,"#{@name} job id=#{@jobs[cycle].id} has hung")
            puts "Cycle #{cycle.strftime("%Y%m%d%H")}:: #{@name} job id=#{@jobs[cycle].id} hung"

            # Delete the hung job
            @jobs[cycle].qdel
            @log.log(cycle,"Killed #{@name} job id=#{@jobs[cycle].id}")            

            # Check resubmit counter
            if @tries > 0 && @ntries[cycle] >= @tries
              @log.log(cycle,"#{@name} has been tried #{@ntries[cycle]} times, giving up")
              puts "Cycle #{cycle.strftime("%Y%m%d%H")}:: #{@name} has been tried #{@ntries[cycle]} times, giving up"
            # Check to make sure throttle is not exceeded
            elsif (@nthrottle >= @throttle && @throttle > 0)
              @log.log(cycle,"#{@name} cannot be resubmitted now because the maximum throttle (#{@throttle}) has been reached")
            # Resubmit if it isn't expired
            elsif !self.expired?(cycle)
              self.submit(cycle)
            end

          end
        end
      end

    end

    # If this task has expired, kill any jobs associated with it and log it
    if self.expired?(cycle)
      @log.log(cycle,"#{@name} has expired!")
      puts "Cycle #{cycle.strftime("%Y%m%d%H")}:: #{@name} has expired"
     
      # Delete the expired job if it exists
      if @jobs[cycle].nil?
        return
      else
        @jobs[cycle].qdel
        @log.log(cycle,"Killed #{@name} job id=#{@jobs[cycle].id} because it expired")            
        puts "Cycle #{cycle.strftime("%Y%m%d%H")}:: Killed #{@name} job id=#{@jobs[cycle].id} because it expired"

        # Decrement the throttle
        if !@jobs[cycle].done?
          @nthrottle-=1
        end

      end

    end

    if @jobs[cycle].nil? || @jobs[cycle].id.nil?

      # Check dependencies to see if this task can be run for cycle
      unless @dependencies.nil?
        return unless @dependencies.resolved?(cycle)
      end

      # Check to make sure throttle is not exceeded
      if (@nthrottle >= @throttle && @throttle > 0)
        @log.log(cycle,"#{@name} cannot be submitted now because the maximum throttle (#{@throttle}) has been reached")
        return
      end

      self.submit(cycle)

    end

  end

  #####################################################
  #
  # halt
  #
  #####################################################
  def halt(cycle)

    begin

      # Attempt to kill the job only if the task is running
      if self.running?(cycle)
        @log.log(cycle,"Attempting to halt #{@name}")
        unless @jobs[cycle].id.nil?
          @jobs[cycle].qdel
          @log.log(cycle,"Killed #{@name} job id=#{@jobs[cycle].id}")
        end
        @log.log(cycle,"#{@name} has been halted")            
      end

    rescue
      @log.log(cycle,"#{$!}")
      return        
    ensure
      @jobs.delete(cycle)
    end

  end


end

$__task__ == __FILE__
end
