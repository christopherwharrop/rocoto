###########################################
#
# Module WorkflowMgr
#
##########################################
module WorkflowMgr

  ##########################################
  #
  # Class COBALTBatchSystem
  #
  ##########################################
  class COBALTBatchSystem

    require 'etc'
    require 'parsedate'
    require 'libxml'
    require 'workflowmgr/utilities'
    require 'tempfile'

    #####################################################
    #
    # initialize
    #
    #####################################################
    def initialize(cobalt_root=nil)

      # Initialize an empty hash for job queue records
      @jobqueue={}

      # Initialize an empty hash for job accounting records
      @jobacct={}

      # Assume the scheduler is up
      @schedup=true

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

        # Populate the job accounting log table if it is empty
        refresh_jobacct(jobid) if @jobacct.empty?

        # Return the jobacct record if there is one
        return @jobacct[jobid] if @jobacct.has_key?(jobid)

        # The state is unavailable since Moab doesn't have the state
        return { :jobid => jobid, :state => "UNKNOWN", :native_state => "Unknown" }
 
      rescue WorkflowMgr::SchedulerDown
        @schedup=false
        return { :jobid => jobid, :state => "UNAVAILABLE", :native_state => "Unavailable" }
      end

    end


#[harrop@cetuslac1 test]$ qsub --help
#Usage: qsub.py --help
#Usage: qsub.py [options] <executable> [<excutable options>]
#
#Refer to man pages for JOBID EXPANSION and SCRIPT JOB DIRECTIVES.
#
#
#Options:
#  --version             show program's version number and exit
#  --help                show this help message and exit
#  -d, --debug           turn on communication debugging
#  -v, --verbose         not used
#  -h, --held            hold this job once submitted
#  --preemptable         make this job preemptable
#  --run_project         set run project flag for this job
#  --disable_preboot     disable script preboot
#  -I, --interactive     run qsub in interactive mode
#  -n NODES, --nodecount=NODES
#                        set job node count
#  --proccount=PROCS     set job proc count
#  -A PROJECT, --project=PROJECT
#                        set project name
#  --cwd=CWD             set current working directory
#  -q QUEUE, --queue=QUEUE
#                        set queue name
#  -M NOTIFY, --notify=NOTIFY
#                        set notification email address
#  --env=ENVS            Set env variables. Refer to man pages for more detail
#                        information.
#  -t WALLTIME, --time=WALLTIME
#                        set walltime (minutes or HH:MM:SS). For max walltime
#                        enter 0.
#  -u UMASK, --umask=UMASK
#                        set umask: octal number default(022)
#  -O OUTPUTPREFIX, --outputprefix=OUTPUTPREFIX
#                        output prefix for error,output or debuglog files
#  -e ERRORPATH, --error=ERRORPATH
#                        set error file path
#  -o OUTPUTPATH, --output=OUTPUTPATH
#                        set output file path
#  -i INPUTFILE, --inputfile=INPUTFILE
#                        set input file
#  --debuglog=COBALT_LOG_FILE
#                        set debug log path file
#  --dependencies=ALL_DEPENDENCIES
#                        set job dependencies (jobid1:jobid2:...:jobidN)
#  --attrs=ATTRS         set attributes (attr1=val1:attr2=val2:...:attrN=valN)
#  --user_list=USER_LIST, --run_users=USER_LIST
#                        set user list (user1:user2:...:userN)
#  --jobname=JOBNAME     Sets Jobname. If this option is not provided then
#                        Jobname will be set to whatever -o option specified.
#  --kernel=KERNEL       set a compute node kernel profile
#  -K KERNELOPTIONS, --kerneloptions=KERNELOPTIONS
#                        set compute node kernel options
#  --ion_kernel=ION_KERNEL
#                        set an IO node kernel profile
#  --ion_kerneloptions=ION_KERNELOPTIONS
#                        set IO node kernel options
#  --mode=MODE           select system mode
#  --geometry=GEOMETRY   set geometry (AxBxCxDxE)
#[harrop@cetuslac1 test]$ 



    #####################################################
    #
    # submit
    #
    #####################################################
    def submit(task)
      # Initialize the submit command
      cmd="qsub --debuglog #{ENV['HOME']}/.rocoto/tmp/\\$jobid.log"
      input="#!/bin/sh\n"

      # Add Cobalt batch system options translated from the generic options specification
      task.attributes.each do |option,value|
        case option
          when :account
            input += "#COBALT -A #{value}\n"
          when :queue            
            input += "#COBALT -q #{value}\n"
          when :cores
            # Ignore this attribute if the "nodes" attribute is present
            next unless task.attributes[:nodes].nil?
#            input += "#COBALT --proccount=#{value}\n"
            input += "#COBALT -n #{(value.to_f / 16.0).ceil}\n"
          when :nodes
            # Remove any occurrences of :tpp=N
            input += "#COBALT -n #{value.gsub(/:tpp=\d+/,"")}\n"
          when :walltime
            input += "#COBALT -t #{value}\n"
          when :memory
#            input += "#PBS -l vmem=#{value}\n"
          when :stdout
            input += "#COBALT -o #{value}\n"
          when :stderr
            input += "#COBALT -e #{value}\n"
          when :join
            input += "#COBALT -o #{value}\n"
            input += "#COBALT -e #{value}\n"
          when :jobname
            input += "#COBALT --jobname #{value}\n"
        end
      end

      task.each_native do |native_line|
next if native_line.empty?
        input += "#COBALT #{native_line}\n"
      end

      # Add export commands to pass environment vars to the job
      unless task.envars.empty?
        varinput=''
        task.envars.each { |name,env|
          varinput += "export #{name}='#{env}'\n"
        }
        input += varinput
      end
#      input+="set -x\n"


      # Build the -F string to pass job script arguments to batch script
      #cmdargs=task.attributes[:command].split[1..-1].join("' '")
      #unless cmdargs.empty?
      #  cmdinput += "\'#{cmdargs}\'\n"
      #end
      #input += "env\n";
      # Add the command to submit
      #input += "'#{task.attributes[:command].split.first}' '#{cmdinput}'"
      input += task.attributes[:command]

      # Get a temporary file name
      tfname=Tempfile.new('qsub.in').path.split("/").last
      tf=File.new("#{ENV['HOME']}/.rocoto/tmp/#{tfname}","w")
      tf.write(input)
      tf.flush()
      tf.chmod(0700)
      tf.close
      
      WorkflowMgr.stderr("Submitting #{task.attributes[:name]} using #{cmd} --mode script #{tf.path} with input {{#{input}}}",4)

      # Run the submit command
      output=`#{cmd} --mode script #{tf.path} 2>&1`.chomp()

      # Parse the output of the submit command
      if output=~/^(\d+)$/
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

      qdel=`qdel #{jobid}`      

    end


private

    #####################################################
    #
    # refresh_jobqueue
    #
    #####################################################
    def refresh_jobqueue

      begin

        # Get the username of this process
        username=Etc.getpwuid(Process.uid).name

        # Run qstat to obtain the current status of queued jobs
        queued_jobs=""
        errors=""
        exit_status=0
        queued_jobs,errors,exit_status=WorkflowMgr.run4("qstat -l -f -u #{username} ",30)

        # Raise SchedulerDown if the showq failed
        raise WorkflowMgr::SchedulerDown,errors unless exit_status==0

        # Return if the showq output is empty
        return if queued_jobs.empty?

        # Parse the XML output of showq, building job status records for each job
#        queued_jobs_doc=LibXML::XML::Parser.string(queued_jobs, :options => LibXML::XML::Parser::Options::HUGE).parse

      rescue LibXML::XML::Error,Timeout::Error,WorkflowMgr::SchedulerDown
        WorkflowMgr.log("#{$!}")
        WorkflowMgr.stderr("#{$!}",3)
        raise WorkflowMgr::SchedulerDown
      end
      
      # For each job, find the various attributes and create a job record
      record = {}
      queued_jobs.each { |job|

        case job
          when /JobID: (\d+)/
            record = {:jobid => $1}
          when /State\s+:\s+(\S+)/
            record[:native_state]=$1
            case record[:native_state]
              when /running/,/starting/,/exiting/
                record[:state] = "RUNNING"
              when /queued/
                record[:state] = "QUEUED"
            end
          when /JobName\s+:\s+(\S+)/
            record[:jobname] = $1
          when /(\S+)\s+:\s+(\S+)/
            record[$1] = $2
        end


#WorkflowMgr.stderr(record.inspect)

  	# Look at all the attributes for this job and build the record
#	job.each_element { |jobstat| 
        
#          case jobstat.name
#            when /Job_Id/
#              record[:jobid]=jobstat.content.split(".").first
#            when /job_state/
#              case jobstat.content
#                when /^Q$/,/^H$/,/^W$/,/^S$/,/^T$/
#    	          record[:state]="QUEUED"
#                when /^R$/,/^E$/
#    	          record[:state]="RUNNING"
#                else
#                  record[:state]="UNKNOWN"
#              end
#              record[:native_state]=jobstat.content
#            when /Job_Name/
#	      record[:jobname]=jobstat.content
#	    when /Job_Owner/
#	      record[:user]=jobstat.content
#           when /Resource_List/       
#              jobstat.each_element { |e|
#                if e.name=='procs'
#                  record[:cores]=e.content.to_i
#                  break
#                end
#            }
#  	    when /queue/
#	      record[:queue]=jobstat.content
#	    when /qtime/
#	      record[:submit_time]=Time.at(jobstat.content.to_i).getgm
#  	    when /start_time/
#              record[:start_time]=Time.at(jobstat.content.to_i).getgm
#	    when /comp_time/
#              record[:end_time]=Time.at(jobstat.content.to_i).getgm
# 	    when /Priority/
#	      record[:priority]=jobstat.content.to_i            
#            when /exit_status/
#              record[:exit_status]=jobstat.content.to_i
#	    else
#              record[jobstat.name]=jobstat.content
#          end  # case jobstat
#  	}  # job.children

        # If the job is complete and has an exit status, change the state to SUCCEEDED or FAILED
        if record[:state]=="UNKNOWN" && !record[:exit_status].nil?
          if record[:exit_status]==0
            record[:state]="SUCCEEDED"
          else
            record[:state]="FAILED"
          end
        end

        # Put the job record in the jobqueue unless it's complete but doesn't have a start time, an end time, and an exit status
        unless record[:state]=="UNKNOWN" || ((record[:state]=="SUCCEEDED" || record[:state]=="FAILED") && (record[:start_time].nil? || record[:end_time].nil?))
          @jobqueue[record[:jobid]]=record
        end

      }  #  queued_jobs.find

      queued_jobs=nil
      GC.start

    end  # job_queue


    #####################################################
    #
    # refresh_jobacct
    #
    #####################################################
    def refresh_jobacct(jobid)

      # Get the username of this process
      username=Etc.getpwuid(Process.uid).name

      # Initialize an empty hash of job records
#      @jobacct={}

      begin

        joblog = IO.readlines("#{ENV['HOME']}/.rocoto/tmp/#{jobid}.log")

        # Return if the joblog output is empty
        return if joblog.empty?

      rescue LibXML::XML::Error,Timeout::Error,WorkflowMgr::SchedulerDown
        WorkflowMgr.log("#{$!}")
        WorkflowMgr.stderr("#{$!}",3)
        raise WorkflowMgr::SchedulerDown        
      end 

      # For each job, find the various attributes and create a job record
      record={:jobid => jobid}
      joblog.each { |line|


#Thu Sep 10 17:35:17 2015 +0000 (UTC) submitted with cwd set to: /gpfs/mira-home/harrop/rocoto/test
#Thu Sep 10 17:35:58 2015 +0000 (UTC) harrop/603816: Initiating boot at location CET-20400-31731-128.
#Thu Sep 10 17:36:56 2015 +0000 (UTC) Info: task completed normally with an exit code of 0; initiating job cleanup and removal

        case line
          when /submitted with cwd set to:/
            record[:submit_time]=Time.gm(*ParseDate.parsedate(line))
          when /Initiating boot at location/
            record[:start_time]=Time.gm(*ParseDate.parsedate(line))
          when /task completed normally with an exit code of (\d+);/
            record[:end_time]=Time.gm(*ParseDate.parsedate(line))
            record[:exit_status] = $1.to_i
        end

#        record[:jobid]=job.attributes['JobID'].split(".").last
#        record[:jobid]=job.attributes['JobID']
#        record[:native_state]=job.attributes['State']
#        record[:jobname]=job.attributes['JobName']
#        record[:user]=job.attributes['User']
#        record[:cores]=job.attributes['ReqProcs'].to_i
#        record[:queue]=job.attributes['Class']
#        record[:submit_time]=Time.at(job.attributes['SubmissionTime'].to_i).getgm
#        record[:start_time]=Time.at(job.attributes['StartTime'].to_i).getgm
#        record[:end_time]=Time.at(job.attributes['CompletionTime'].to_i).getgm
#        record[:duration]=job.attributes['AWDuration'].to_i
#        record[:priority]=job.attributes['StartPriority'].to_i
#        if job.attributes['State']=~/^Removed/ || job.attributes['CompletionCode']=~/^CNCLD/
#          record[:exit_status]=255
#          else
#          record[:exit_status]=job.attributes['CompletionCode'].to_i
#        end

      }

      if record[:exit_status]==0
        record[:state]="SUCCEEDED"
      else
        record[:state]="FAILED"
      end

      # Add the record if it hasn't already been added
      @jobacct[record[:jobid]]=record unless @jobacct.has_key?(record[:jobid])

      GC.start
    
    end

  end  # class

end  # module

