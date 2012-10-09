##########################################
#
# Module WorkflowMgr
#
##########################################
module WorkflowMgr

  ##########################################
  #
  # Class LSFBatchSystem 
  #
  ##########################################
  class LSFBatchSystem

    require 'fileutils'
    require 'etc'

    @@qstat_refresh_rate=30
    @@max_history=3600*1

    #####################################################
    #
    # initialize
    #
    #####################################################
    def initialize


      # Initialize an empty hash for job queue records
      @jobqueue=nil

      # Initialize an empty hash for job accounting records
      @jobacct={}

      # Initialize the number of accounting files examined to produce the jobacct hash
      @nacctfiles=1

    end


    #####################################################
    #
    # status
    #
    #####################################################
    def status(jobid)

      # Populate the jobs status table if it is empty
      refresh_jobqueue if @jobqueue.nil?

      # Return the jobqueue record if there is one
      return @jobqueue[jobid] if @jobqueue.has_key?(jobid)

      # If we didn't find the job in the jobqueue, look for it in the accounting records

      # Populate the job accounting log table if it is empty
      refresh_jobacct if @jobacct.empty?

      # Return the jobacct record if there is one
      return @jobacct[jobid] if @jobacct.has_key?(jobid)

      # If we still didn't find the job, look at all accounting files if we haven't already
      if @nacctfiles != 25
	refresh_jobacct(25)
	return @jobacct[jobid] if @jobacct.has_key?(jobid)
      end

      # We didn't find the job, so return an uknown status record
      return { :jobid => jobid, :state => "UNKNOWN", :native_state => "Unknown" }

    end

    #####################################################
    #
    # submit
    #
    #####################################################
    def submit(task)

      # Initialize the submit command
      cmd="bsub"

      # Add LSF batch system options translated from the generic options specification
      task.attributes.each do |option,value|

        case option
          when :account
            cmd += " -P #{value}"
          when :queue            
            cmd += " -q #{value}"
          when :cores
            cmd += " -n #{value}"
          when :walltime
            hhmm=WorkflowMgr.seconds_to_hhmm(WorkflowMgr.ddhhmmss_to_seconds(value))
            cmd += " -W #{hhmm}"
          when :memory
            units=value[-1,1]
            amount=value[0..-2].to_i
            case units
              when /B|b/
                amount=(amount / 1024.0).ceil
              when /K|k/
                amount=amount.ceil
              when /M|m/
                amount=(amount * 1024.0).ceil
              when /G|g/
                amount=(amount * 1024.0 * 1024.0).ceil
              when /[0-9]/
                amount=(value.to_i / 1024.0).ceil
            end          
            cmd += " -M #{amount}"
          when :stdout
	    FileUtils.mkdir_p(File.dirname(value))
            cmd += " -o #{value}"
          when :stderr
	    FileUtils.mkdir_p(File.dirname(value))
            cmd += " -e #{value}"
          when :join
	    FileUtils.mkdir_p(File.dirname(value))
            cmd += " -o #{value}"           
          when :jobname
            cmd += " -J #{value}"
          when :native
	    cmd += " #{value}"
        end
      end

      # LSF does not have an option to pass environment vars
      # Instead, the vars must be set in the environment before submission
      task.envars.each { |name,env|
        if env.nil?
          ENV[name]=""
        else
          ENV[name]=env
        end
      }

      # Add the command to submit
      cmd += " #{task.attributes[:command]}"

      # Run the submit command
      output=`#{cmd} 2>&1`.chomp

      # Parse the output of the submit command
      if output=~/Job <(\d+)> is submitted to queue/
        return $1,output
      else
 	return nil,output
      end

    end


    #####################################################
    #
    # delete
    #
    #####################################################
    def delete(jobid)

      qdel=`bkill #{jobid}`      

    end

private

    #####################################################
    #
    # refresh_jobqueue
    #
    #####################################################
    def refresh_jobqueue

      # Initialize an empty hash for job queue records
      @jobqueue={}

      # run bjobs to obtain the current status of queued jobs
      queued_jobs=`bjobs -w 2>&1`

      # Parse the output of bjobs, building job status records for each job
      queued_jobs.split(/\n/).each { |s|

        # Skip the header line
	next if s=~/^JOBID/

        # Split the fields of the bjobs output
        jobattributes=s.strip.split(/\s+/)

        # Build a job record from the attributes
	if jobattributes.size == 1
          # This is a continuation of the exec host line, which we don't need
          next
        else
        
          # Initialize an empty job record 
          record={} 

          # Record the fields
          record[:jobid]=jobattributes[0]
          record[:user]=jobattributes[1]
          record[:native_state]=jobattributes[2]
          case jobattributes[2]
            when /^PEND$/
              record[:state]="QUEUED"
            when /^RUN$/
              record[:state]="RUNNING"
            else
              record[:state]="UNKNOWN"   
          end          
          record[:queue]=jobattributes[3]
          record[:jobname]=jobattributes[6]
          record[:cores]=nil
          submit_time=ParseDate.parsedate(jobattributes[-3..-1].join(" "),true)
          if submit_time[0].nil?
            now=Time.now
            submit_time[0]=now.year
            if Time.local(*submit_time) > now
              submit_time[0]=now.year-1
            end
          end
          record[:submit_time]=Time.local(*submit_time).getgm
          record[:start_time]=nil
          record[:priority]=nil

          # Put the job record in the jobqueue
	  @jobqueue[record[:jobid]]=record

        end

      }

    end


    #####################################################
    #
    # refresh_jobacct
    #
    #####################################################
    def refresh_jobacct(nacctfiles=1)

      # Get the username of this process
      username=Etc.getpwuid(Process.uid).name

      # Initialize an empty hash of job records
      @jobacct={}

      # Run bhist to obtain the current status of queued jobs
      output=`bhist -n #{nacctfiles} -l -d -w 2>&1`

      # Build job records from output of bhist
      output.split(/^-{10,}\n$/).each { |s|

        record={}

        # Try to format the record such that it is easier to parse
        recordstring=s.strip
        recordstring.gsub!(/\n\s{3,}/,'')
        recordstring.split(/\n+/).each { |event|
          case event.strip
            when /^Job <(\d+)>, Job Name <(\w+)>, User <(\w+)>,/
              record[:jobid]=$1
              record[:jobname]=$2
              record[:user]=$3
              record[:native_state]="DONE"
            when /(\w+\s+\w+\s+\d+\s+\d+:\d+:\d+): Submitted from host <\w+>, to Queue <(\w+)>,/
              timestamp=ParseDate.parsedate($1,true)
              if timestamp[0].nil?
                now=Time.now
                timestamp[0]=now.year
                if Time.local(*timestamp) > now
                  timestamp[0]=now.year-1
                end
              end
              record[:submit_time]=Time.local(*timestamp).getgm
              record[:queue]=$2        
            when /(\w+\s+\w+\s+\d+\s+\d+:\d+:\d+): Dispatched to /
              timestamp=ParseDate.parsedate($1,true)
              if timestamp[0].nil?
                now=Time.now
                timestamp[0]=now.year
                if Time.local(*timestamp) > now
                  timestamp[0]=now.year-1
                end
              end
              record[:start_time]=Time.local(*timestamp).getgm
            when /(\w+\s+\w+\s+\d+\s+\d+:\d+:\d+): Done successfully. /
              timestamp=ParseDate.parsedate($1,true)
              if timestamp[0].nil?
                now=Time.now
                timestamp[0]=now.year
                if Time.local(*timestamp) > now
                  timestamp[0]=now.year-1
                end
              end
              record[:end_time]=Time.local(*timestamp).getgm
              record[:exit_status]=0             
              record[:state]="SUCCEEDED"
            when /(\w+\s+\w+\s+\d+\s+\d+:\d+:\d+): Exited with exit code (\d+)/
              timestamp=ParseDate.parsedate($1,true)
              if timestamp[0].nil?
                now=Time.now
                timestamp[0]=now.year
                if Time.local(*timestamp) > now
                  timestamp[0]=now.year-1
                end
              end
              record[:end_time]=Time.local(*timestamp).getgm
              record[:exit_status]=$2.to_i             
              record[:state]="FAILED"
            else
          end
        }

        @jobacct[record[:jobid]]=record unless @jobacct.has_key?(record[:jobid])

      }        

      # Update the number of accounting files examined to produce the jobacct hash
      @nacctfiles=nacctfiles

    end

  end

end
