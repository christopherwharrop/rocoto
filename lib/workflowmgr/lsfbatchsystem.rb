##########################################
#
# Module WorkflowMgr
#
##########################################
module WorkflowMgr

  require 'workflowmgr/batchsystem'

  ##########################################
  #
  # Class LSFBatchSystem 
  #
  ##########################################
  class LSFBatchSystem < BatchSystem

    require 'workflowmgr/utilities'
    require 'fileutils'
    require 'etc'
    require 'tempfile'

    attr_accessor :unhold_jobs

    @@qstat_refresh_rate=30
    @@max_history=3600*1

    #####################################################
    #
    # initialize
    #
    #####################################################
    def initialize(unhold_jobs_default=false,should_vanquish_undead=false)

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

      # Should we try to unhold PSUSP status jobs?  Normally we
      # shouldn't, but on WCOSS Cray, the broken combination of LSF
      # and ALPS sometimes places jobs randomly in PSUSP status.
      @should_unhold_jobs=unhold_jobs_default

      @should_vanquish_undead=should_vanquish_undead
    end


    #####################################################
    #
    # statuses
    #
    #####################################################
    def statuses(jobids)

      begin

        raise WorkflowMgr::SchedulerDown unless @schedup

        # Initialize statuses to UNAVAILABLE
        jobStatuses={}
        jobids.each do |jobid|
          jobStatuses[jobid] = { :jobid => jobid, :state => "UNAVAILABLE", :native_state => "Unavailable" }
        end

        jobids.each do |jobid|
          jobStatuses[jobid] = self.status(jobid)
        end

      rescue WorkflowMgr::SchedulerDown
        @schedup=false
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

        raise WorkflowMgr::SchedulerDown unless @schedup

        if not @should_unhold_jobs
          # Populate the jobs status table if it is empty
          refresh_jobqueue if @jobqueue.empty?

          # Return the jobqueue record if there is one
          return @jobqueue[jobid] if @jobqueue.has_key?(jobid)

          # If we didn't find the job in the jobqueue, look for it in the accounting records
        end
        
        refresh_bjobs if @bjobs.empty?
        vanquish_undead(@bjobs,jobid) if @should_vanquish_undead
        return unhold_job(@bjobs,jobid) if @bjobs.has_key?(jobid)

        # Populate the job accounting log table if it is empty
        refresh_jobacct if @bhist.empty?

        # Return the jobacct record if there is one
        vanquish_undead(@bhist,jobid) if @should_vanquish_undead
        return unhold_job(@bhist,jobid) if @bhist.has_key?(jobid)

        # If we still didn't find the job, look at all accounting files if we haven't already
        if @nacctfiles != 25
          refresh_jobacct(25)
          vanquish_undead(@bhist,jobid) if @should_vanquish_undead
          return unhold_job(@bhist,jobid) if @bhist.has_key?(jobid)
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

      # Build up the string of environment settings
      envstr="#!/bin/sh\n"
      task.envars.each { |name,env|
        if env.nil?
          envstr += "export #{name}\n"
        else
          envstr += "export #{name}='#{env}'\n"
        end
      }

      # Add LSF batch system options translated from the generic options specification
      task.attributes.each do |option,value|

         if value.is_a?(String)
           if value.empty?
             WorkflowMgr.stderr("WARNING: <#{option}> has empty content and is ignored", 1)
             next
           end
        end
        case option
          when :account
            cmd += " -P #{value}"
          when :nodesize
            # Nothing to do
          when :queue            
            cmd += " -q #{value}"
          when :partition
            WorkflowMgr.stderr("WARNING: the <partition> tag is not supported for LSF.", 1)
            WorkflowMgr.log("WARNING: the <partition> tag is not supported for LSF.", 1)
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
              envstr += "export ROCOTO_TASK_GEO='#{task_geometry}'\n"
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
            envstr += "export ROCOTO_TASK_GEO='#{task_geometry}'\n"

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

      # Add the command to submit
      cmd += " #{rocotodir}/sbin/lsfwrapper.sh #{task.attributes[:command]}"

      # Build a script to set env vars and then call bsub to submit the job
      tf=Tempfile.new('bsub.wrapper')
      tf.write(envstr + cmd)
      tf.flush()

      # Run the submit command script
      output=`/bin/sh #{tf.path} 2>&1`.chomp

      WorkflowMgr.log("Submitted #{task.attributes[:name]} using '/bin/sh #{tf.path} 2>&1' with input {{#{envstr + cmd}}}")
      WorkflowMgr.stderr("Submitted #{task.attributes[:name]} using '/bin/sh #{tf.path} 2>&1' with input {{#{envstr + cmd}}}",4)

      # Parse the output of the submit command
      if output=~/Job <(\d+)> is submitted to (default )*queue/
        return $1,output
      else
 	return nil,output
      end

    end


    #####################################################
    #
    # vanquish_undead - if a job is falsely reported as
    #   running, but has actually hung, kill it and list
    #   it as failed.  This is a workaround for WCOSS.
    #   Only called if @should_vanquish_undead=true
    #
    #####################################################
    def vanquish_undead(bjobs,jobid)
      return unless @should_vanquish_undead
      job=bjobs[jobid]
      return nil if job.nil?



      if not job[:reservation_time].nil? and not job[:lsf_runlimit].nil?
        now=Time.now
        reservation_age=now-job[:reservation_time]
        runlimit=job[:lsf_runlimit]
        past=reservation_age/60.0-runlimit
        if past > 10
          WorkflowMgr.stderr("#{job[:jobid]}: Is a zombie, falsely reported as running.  Will bkill job and list as failed with exit status 1000.  Info: now=#{now} res age=#{reservation_age} runlimit=#{runlimit} past=#{past}.",1)
          job[:state]='FAILED'
          job[:native_state]='ZOMBIE_RUNNING'
          job[:exit_status]=100
          job[:end_time]=now
        else
          WorkflowMgr.stderr("#{job[:jobid]}: Running for #{(reservation_age/60).floor} of #{runlimit} reservation (past=#{past}).",1)
        end
      end
      return nil
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
        WorkflowMgr.stderr("error running bjobs: #{$!}",3)
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
              next
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
    # final_update_record: given a record from within
    #    run_bhist_bjobs, makes additional changes to the
    #    record based on derived information.  This is
    #    intended for subclasses' use.
    #
    #####################################################
    def final_update_record(record,jobacct)
      # do nothing
    end
    
    #####################################################
    #
    # unhold_job: a workaround on WCOSS Cray.  This
    #    calls bresume to release the user hold on any
    #    held jobs.  It also modifies the queue :state
    #    from USERHOLD to QUEUED.  Does nothing if
    #    @should_unhold_jobs is false.
    #
    #####################################################
    def unhold_job(joblist,jobid)
      job=joblist[jobid]
      
      return job unless @should_unhold_jobs

      # Nothing to do unless job state is userhold
      return job if job[:state] != 'USERHOLD'
      
      cmd = "bresume -u #{ENV['USER']} #{job[:jobid]}"
      
      # When jobs are held, resume them:
      queued_jobs,errors,exit_status=WorkflowMgr.run4(cmd,30)
      
      if exit_status==0
        # If bresume works, assume the jobs are running now:
        WorkflowMgr.stderr("Job #{job[:jobid]} #{job[:jobname]} resumed.")
        job[:state]='QUEUED'
        job[:native_state]='QUEUED'
      else
        # If bresume fails, the job status is now unknown:
        WorkflowMgr.stderr("Exit status #{exit_status} from #{cmd}.  Job #{job[:jobid]} status now unknown.")
        job[:state]='UNKNOWN'
        job[:native_state]='UNKNOWN'
      end
      return job
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
          WorkflowMgr.stderr("bjobs -l -a ",10)
          completed_jobs,errors,exit_status=WorkflowMgr.run4("bjobs -l -a",timeout)
        else
          WorkflowMgr.stderr("bhist -n #{nacctfiles} -l -d -w ",10)
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
        WorkflowMgr.log("Error running bhist or bjobs: #{$!}")
        WorkflowMgr.stderr("Error running bhist or bjobs: #{$!}",3)
        raise WorkflowMgr::SchedulerDown
      end
      # Build job records from output of bhist
      completed_jobs.split(/^-{10,}\n$/).each { |s|

        record={}

        # Try to format the record such that it is easier to parse
        recordstring=s.strip
        # This one is not in a time record, so we handle it separately:
        if /^\s*([0-9.]+) min of/.match(s)
            record[:lsf_runlimit]=$1.to_f
        end
        recordstring.gsub!(/\n\s{3,}/,'')
        recordstring.split(/\n+/).each { |event|
          case event.strip
#Job <216811>, Job Name <rt_test_gsm_t126_mom5_cice5_2015040100_2day_cold>, User<emc.nemspara>, Project <GFS-T2O>,
            when /^Job *<(\d+)>, *(Job Name *<([^>]+)>,)? *User *<([^>]+)>,/
              record[:jobid]=$1
              record[:jobname]=$3
              record[:user]=$4
              if(event.strip=~/Extsched <([^>]*)>/)
                record[:extsched]=$1
              end
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
                when 'EXIT'
                  record[:native_state]='EXIT'
                when 'DONE'
                  record[:native_state]='DONE'
                when 'PSUSP'
                  record[:state]='USERHOLD'
                else
                  record[:state]='UNKNOWN'
                end
              else
                record[:native_state]="DONE"
              end
            when /(\w+\s+\w+\s+\d+\s+\d+:\d+:\d+)(\s+\d\d\d\d)*: Submitted from host <[^>]+>, to Queue <([^>]+)>,/
              record[:submit_time]=lsf_time($1)
              record[:queue]=$2        
            when /(\w+\s+\w+\s+\d+\s+\d+:\d+:\d+)(\s+\d\d\d\d)*: Submitted from host <[^>]+>, CWD/
              record[:submit_time]=lsf_time($1)
            when /(\w+\s+\w+\s+\d+\s+\d+:\d+:\d+)(\s+\d\d\d\d)*: (Dispatched to|Started) /
              record[:start_time]=lsf_time($1)
            when /(\w+\s+\w+\s+\d+\s+\d+:\d+:\d+)(\s+\d\d\d\d)*: reservation_id *= *([0-9A-Za-z_.]+) *;/
              record[:reservation_id]=$3
              record[:reservation_time]=lsf_time($1)
            when /(\w+\s+\w+\s+\d+\s+\d+:\d+:\d+)(\s+\d\d\d\d)*: Done successfully. /
              record[:end_time]=lsf_time($1)
              record[:exit_status]=0             
              record[:state]="SUCCEEDED"
            when /(\w+\s+\w+\s+\d+\s+\d+:\d+:\d+)(\s+\d\d\d\d)*: Exited with exit code (\d+)/,/(\w+\s+\w+\s+\d+\s+\d+:\d+:\d+)(\s+\d\d\d\d)*: Exited by signal (\d+)/
              record[:end_time]=lsf_time($1)
              record[:exit_status]=$3.to_i             
              record[:state]="FAILED"
            when /(\w+\s+\w+\s+\d+\s+\d+:\d+:\d+)(\s+\d\d\d\d)*: Exited; job has been forced to exit with exit code (\d+)/
              record[:end_time]=lsf_time($1)
              record[:exit_status]=$3.to_i             
              record[:state]="FAILED"
            when /(\w+\s+\w+\s+\d+\s+\d+:\d+:\d+)(\s+\d\d\d\d)*: Exited by LSF signal ([A-Za-z0-9_]+)/
              record[:end_time]=lsf_time($1)
              record[:exit_status]=-1
              record[:native_state]=$3
              record[:state]="FAILED"
            when /(\w+\s+\w+\s+\d+\s+\d+:\d+:\d+)(\s+\d\d\d\d)*: Exited\./
              record[:end_time]=lsf_time($1)
              record[:exit_status]=255
              record[:state]="FAILED"
            else
          end
        }

        final_update_record(record,jobacct)

        if !jobacct.has_key?(record[:jobid])
          if record.has_key?(:state) and record[:state]!='UNKNOWN'
            jobacct[record[:jobid]]=record
          end
        end

      }        

      return jobacct
    end

    def lsf_time(str,now=nil)
      timestamp=ParseDate.parsedate(str,true)
      if timestamp[0].nil?
        now=Time.now if now.nil?
        timestamp[0]=now.year
        if Time.local(*timestamp) > now
          timestamp[0]=now.year-1
        end
      end
      return Time.local(*timestamp).getgm
    end
    
  end

end
