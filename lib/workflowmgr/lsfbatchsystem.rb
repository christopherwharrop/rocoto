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

    require 'workflowmgr/utilities'
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
      @jobqueue={}

      # Initialize an empty hash for job accounting records
      @bhist={}

      # Initialize the number of accounting files examined to produce the jobacct hash
      @nacctfiles=1

      # Assume the scheduler is up
      @schedup=true

      # Similar to the jobacct, but from bjobs
      @bjobs={}

    end


    #####################################################
    #
    # status
    #
    #####################################################
    def status(jobid)

      begin

        raise WorkflowMgr::SchedulerDown unless @schedup
        # Populate the jobs status table if it is empty
        refresh_jobqueue if @jobqueue.empty?

        # Return the jobqueue record if there is one
        return @jobqueue[jobid] if @jobqueue.has_key?(jobid)

        # If we didn't find the job in the jobqueue, look for it in the accounting records


        refresh_bjobs if @bjobs.empty?
        return @bjobs[jobid] if @bjobs.has_key?(jobid)


        # Populate the job accounting log table if it is empty
        refresh_jobacct if @bhist.empty?

        # Return the jobacct record if there is one
        return @bhist[jobid] if @bhist.has_key?(jobid)

        # If we still didn't find the job, look at all accounting files if we haven't already
        if @nacctfiles != 25
          refresh_jobacct(25)
          return @bhist[jobid] if @bhist.has_key?(jobid)
        end

        # We didn't find the job, so return an uknown status record
        return { :jobid => jobid, :state => "UNKNOWN", :native_state => "Unknown" }

      rescue WorkflowMgr::SchedulerDown => ex
        @schedup=false
        WorkflowMgr.stderr("Received SchedulerDown '#{ex.message}'",2)
        return { :jobid => jobid, :state => "UNKNOWN", :native_state => "Unknown" }
      end

    end

    #####################################################
    #
    # submit
    #
    #####################################################
    def submit(task)

      # Initialize the submit command
      cmd="bsub"

      # Get the Rocoto installation directory
      rocotodir=File.dirname(File.dirname(File.expand_path(File.dirname(__FILE__))))

      if ENV.has_key?('ROCOTO_TASK_GEO')
        ENV.delete('ROCOTO_TASK_GEO')
      end

      # Add LSF batch system options translated from the generic options specification
      task.attributes.each do |option,value|

        case option
          when :account
            cmd += " -P #{value}"
          when :nodesize
            # Nothing to do
          when :queue            
            cmd += " -q #{value}"
          when :cores  
            next unless task.attributes[:nodes].nil?          
            wantcores=value.to_s.to_i
            if task.attributes[:nodesize].nil?
              cmd += " -n #{value}"
            else
              nodesize=task.attributes[:nodesize].to_i
              if wantcores>nodesize
                rounddown=wantcores/nodesize
                roundup=(wantcores+nodesize-1)/nodesize
                lowcores=wantcores/roundup
                overcores=lowcores*roundup
                bignodes=wantcores-overcores
                littlenodes=roundup-bignodes
                totalcores=bignodes*(lowcores+1) + littlenodes*(lowcores)
                if bignodes>0
                  span="-R span[ptile=#{lowcores+1}]"
                else
                  span="-R span[ptile=#{lowcores}]"
                end
                task_geometry = '{'
                iproc=0
                for inode in (0..(bignodes-1))
                  task_geometry += '('+(iproc..(iproc+lowcores)).to_a.join(',')+')'
                  iproc+=lowcores+1
                end
                for inode in (0..(littlenodes-1))
                  task_geometry += '('+(iproc..(iproc+lowcores-1)).to_a.join(',')+')'
                  iproc+=lowcores
                end
                task_geometry += '}'
                if bignodes>0
                  nval=(bignodes+littlenodes)*(lowcores+1)
                  span=lowcores+1
                else
                  nval=littlenodes*lowcores
                  span=lowcores
                end
              else
                span=wantcores
                task_geometry="{(#{(0..(wantcores-1)).to_a.join(',')})}"
                nval=wantcores
              end
              cmd += " -R span[ptile=#{span}]"
              cmd += " -n #{nval}"
              ENV["ROCOTO_TASK_GEO"]=task_geometry
            end
          when :nodes
            # Get largest ppn*tpp to calculate ptile
            # -n is ptile * number of nodes
            ptile=0
            nnodes=0
            task_index=0
            task_geometry="{"
            value.split("+").each { |nodespec|
              resources=nodespec.split(":")
              mynodes=resources.shift.to_i
              nnodes+=mynodes
              ppn=0
              tpp=1
              resources.each { |resource|
                case resource
                  when /ppn=(\d+)/
                    ppn=$1.to_i
                  when /tpp=(\d+)/
                    tpp=$1.to_i
                end
              }
              procs=ppn*tpp
              ptile=procs if procs > ptile
              appendme=''
              inode=1
              while inode<=mynodes do
                appendme+="(#{(task_index..task_index+ppn-1).to_a.join(",")})"
                task_index+=ppn
                inode+=1
              end
              task_geometry += appendme
            }
            task_geometry+="}"

            # Add the ptile to the command
            cmd += " -R span[ptile=#{ptile}]"

            # Add -n to the command
            cmd += " -n #{nnodes*ptile}"
 
            # Setenv the LSB_PJL_TASK_GEOMETRY to specify task layout
            ENV["ROCOTO_TASK_GEO"]=task_geometry
          when :walltime
            hhmm=WorkflowMgr.seconds_to_hhmm(WorkflowMgr.ddhhmmss_to_seconds(value))
            cmd += " -W #{hhmm}"
          when :memory
            units=value[-1,1]
            amount=value[0..-2].to_i
            case units
              when /B|b/
                amount=(amount / 1024.0 / 1024.0).ceil
              when /K|k/
                amount=(amount / 1024.0).ceil
              when /M|m/
                amount=amount.ceil
              when /G|g/
                amount=(amount * 1024.0).ceil
              when /[0-9]/
                amount=(value.to_i / 1024.0 / 1024.0).ceil
            end          
            if amount>0
              cmd += " -R rusage[mem=#{amount}]"
            end
          when :stdout
            cmd += " -o #{value}"
          when :stderr
            cmd += " -e #{value}"
          when :join
            cmd += " -o #{value}"           
          when :jobname
            cmd += " -J #{value}"
        end
      end

      inl=0
      task.each_native do |native_line|
        cmd += " #{native_line}"
        inl+=1
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
      cmd += " #{rocotodir}/sbin/lsfwrapper.sh #{task.attributes[:command]}"
      #WorkflowMgr.stderr("Submitted #{task.attributes[:name]} using '#{cmd}'",4)

      # Run the submit command
      output=`#{cmd} 2>&1`.chomp

      WorkflowMgr.log("Submitted #{task.attributes[:name]} using #{cmd} 2>&1 ==> #{output}")
      WorkflowMgr.stderr("Submitted #{task.attributes[:name]} using #{cmd} 2>&1 ==> #{output}",4)

      # Parse the output of the submit command
      if output=~/Job <(\d+)> is submitted to (default )*queue/
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
      begin

        # run bjobs to obtain the current status of queued jobs
        queued_jobs=""
        errors=""
        exit_status=0
        queued_jobs,errors,exit_status=WorkflowMgr.run4("bjobs -w",30)

        # Raise SchedulerDown if the bjobs failed
        unless exit_status==0
          WorkflowMgr.stderr("Exit status #{exit_status} from bjobs.",2)
          # Raise SchedulerDown if the bhist failed
          raise WorkflowMgr::SchedulerDown,errors
        end

        # Return if the bjobs output is empty
        return if queued_jobs.empty? || queued_jobs=~/^No unfinished job found$/

      rescue Timeout::Error,WorkflowMgr::SchedulerDown
        WorkflowMgr.log("#{$!}")
        WorkflowMgr.stderr("#{$!}",3)
        raise WorkflowMgr::SchedulerDown
      end

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
    # refresh_bjobs - runs bjobs, updates @bjobs
    #
    #####################################################
    def refresh_bjobs()
      j=run_bhist_bjobs(0,true)
      @bjobs=j
    end

    #####################################################
    #
    # refresh_jobacct - runs bhist, updates @bhist
    #
    #####################################################
    def refresh_jobacct(nacctfiles=1)
      j=run_bhist_bjobs(nacctfiles,false)
      @bhist=j
      @nacctfiles=nacctfiles
    end

    #####################################################
    #
    # run_bhist_bjobs - runs and parses the output from:
    #     bjobs=true:  bjobs -l
    #     bjobs=false: bhist -n #{nacctfiles} -l -d -w
    # Returns a tuple (n,j) where n is nacctfiles, and
    # j is a hash of the results.
    #
    #####################################################
    def run_bhist_bjobs(nacctfiles=1,bjobs=true)

      # Get the username of this process
      username=Etc.getpwuid(Process.uid).name

      # Initialize an empty hash of job records
      jobacct={}

      begin

        # Run bhist or bjobs to obtain the current status of queued jobs
        completed_jobs=""
        errors=""
        exit_status=0
        timeout=nacctfiles==1 ? 30 : 90
        if(bjobs) then
          completed_jobs,errors,exit_status=WorkflowMgr.run4("bjobs -l -a",timeout)
        else
          completed_jobs,errors,exit_status=WorkflowMgr.run4("bhist -n #{nacctfiles} -l -d -w",timeout)
        end

        # Return if the bhist output is empty
        return {} if completed_jobs.empty? || completed_jobs=~/^No matching job found$/
        unless exit_status==0
          if bjobs then
            WorkflowMgr.stderr("Exit status #{exit_status} from bjobs.",2)
          else
            WorkflowMgr.stderr("Exit status #{exit_status} from bhist.",2)
          end
          # Raise SchedulerDown if the bhist failed
          raise WorkflowMgr::SchedulerDown,errors
        end

      rescue Timeout::Error,WorkflowMgr::SchedulerDown
        WorkflowMgr.log("#{$!}")
        WorkflowMgr.stderr("#{$!}",3)
        raise WorkflowMgr::SchedulerDown
      end
      # Build job records from output of bhist
      completed_jobs.split(/^-{10,}\n$/).each { |s|

        record={}

        # Try to format the record such that it is easier to parse
        recordstring=s.strip
        recordstring.gsub!(/\n\s{3,}/,'')
        recordstring.split(/\n+/).each { |event|
          case event.strip
            when /^Job <(\d+)>,( Job Name <([^>]+)>,)* User <([^>]+)>,/
              record[:jobid]=$1
              record[:jobname]=$3
              record[:user]=$4
              if(event.strip=~/Queue <([^>]*)>/)
                record[:queue]=$1
              end
              if(event.strip=~/Status <([^>]*)>/)
                state=$1
                record[:native_state]=state
                case state
                when 'RUN'
                  record[:state]='RUNNING'
                when 'PEND'
                  record[:state]='QUEUED'
                when 'DONE'
                  record[:native_state]='DONE'
                else
                  record[:state]='UNKNOWN'
                end
              else
                record[:native_state]="DONE"
              end
            when /(\w+\s+\w+\s+\d+\s+\d+:\d+:\d+)(\s+\d\d\d\d)*: Submitted from host <[^>]+>, to Queue <([^>]+)>,/
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
            when /(\w+\s+\w+\s+\d+\s+\d+:\d+:\d+)(\s+\d\d\d\d)*: Submitted from host <[^>]+>, CWD/
              timestamp=ParseDate.parsedate($1,true)
              if timestamp[0].nil?
                now=Time.now
                timestamp[0]=now.year
                if Time.local(*timestamp) > now
                  timestamp[0]=now.year-1
                end
              end
              record[:submit_time]=Time.local(*timestamp).getgm
            when /(\w+\s+\w+\s+\d+\s+\d+:\d+:\d+)(\s+\d\d\d\d)*: (Dispatched to|Started on) /
              timestamp=ParseDate.parsedate($1,true)
              if timestamp[0].nil?
                now=Time.now
                timestamp[0]=now.year
                if Time.local(*timestamp) > now
                  timestamp[0]=now.year-1
                end
              end
              record[:start_time]=Time.local(*timestamp).getgm
            when /(\w+\s+\w+\s+\d+\s+\d+:\d+:\d+)(\s+\d\d\d\d)*: Done successfully. /
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
            when /(\w+\s+\w+\s+\d+\s+\d+:\d+:\d+)(\s+\d\d\d\d)*: Exited with exit code (\d+)/,/(\w+\s+\w+\s+\d+\s+\d+:\d+:\d+)(\s+\d\d\d\d)*: Exited by signal (\d+)/
              timestamp=ParseDate.parsedate($1,true)
              if timestamp[0].nil?
                now=Time.now
                timestamp[0]=now.year
                if Time.local(*timestamp) > now
                  timestamp[0]=now.year-1
                end
              end
              record[:end_time]=Time.local(*timestamp).getgm
              record[:exit_status]=$3.to_i             
              record[:state]="FAILED"
            when /(\w+\s+\w+\s+\d+\s+\d+:\d+:\d+)(\s+\d\d\d\d)*: Exited; job has been forced to exit with exit code (\d+)/
              timestamp=ParseDate.parsedate($1,true)
              if timestamp[0].nil?
                now=Time.now
                timestamp[0]=now.year
                if Time.local(*timestamp) > now
                  timestamp[0]=now.year-1
                end
              end
              record[:end_time]=Time.local(*timestamp).getgm
              record[:exit_status]=$3.to_i             
              record[:state]="FAILED"
            when /(\w+\s+\w+\s+\d+\s+\d+:\d+:\d+)(\s+\d\d\d\d)*: Exited\./
              timestamp=ParseDate.parsedate($1,true)
              if timestamp[0].nil?
                now=Time.now
                timestamp[0]=now.year
                if Time.local(*timestamp) > now
                  timestamp[0]=now.year-1
                end
              end
              record[:end_time]=Time.local(*timestamp).getgm
              record[:exit_status]=255
              record[:state]="FAILED"
            else
          end
        }

        if !jobacct.has_key?(record[:jobid])
          jobacct[record[:jobid]]=record
        end

      }        

      return jobacct
    end

  end

end
