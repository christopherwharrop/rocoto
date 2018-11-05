##########################################
#
# Module WorkflowMgr
#
##########################################
module WorkflowMgr

  require 'workflowmgr/lsfbatchsystem'

  ##########################################
  #
  # Class LSFCrayBatchSystem
  #
  ##########################################
  class LSFCRAYBatchSystem < LSFBatchSystem

    require 'workflowmgr/utilities'
    require 'fileutils'
    require 'etc'
    require 'tempfile'

    @@features = { 
      :shared => true,
      :exclusive => true
    }

    def self.feature?(flag)
      return !!@@features[flag]
    end

    #####################################################
    #
    # initialize
    #
    #####################################################
    def initialize
      # Enable Cray workarounds in parent class:
      super(true,false)
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

      totalcores=0 # Total requested cores, used to trigger -n
      nodesize=24  # Users will override with <nodesize> attribute.
      memoryoption=nil # Contains memory option if -n is used
      
      # First pass over attributes: get node size and everything else
      # that is not a request for cores/nodes:
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
            nodesize=value
          when :queue            
            cmd += " -q #{value}"
          when :partition
            WorkflowMgr.stderr("WARNING: the <partition> tag is not supported for LSF.", 1)
            WorkflowMgr.log("WARNING: the <partition> tag is not supported for LSF.", 1)
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
              memoryoption = "#{amount}"
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

      nodes=0
      fnodesize=Float(nodesize)
      
      # Second pass over attributes: figure out total number of
      # requested nodes.
      spanguess=0
      task.attributes.each do |option,value|
        case option
        when :cores
          unless task.attributes[:nodes].nil?
            next
          end
          totalcores += value.to_i
          nodes += (value.to_f/nodesize.to_f).ceil.to_i

        when :nodes
          value.split('+').each { |nodespec|
            resources=nodespec.split(':ppn=')
            mynodes=resources.shift.to_i
            mycores=resources.shift.to_i
            ## BUG: This does not handle threads correctly
            nodes+=mynodes
            totalcores += mynodes*mycores
            spanguess=[ spanguess, mycores ].max
          }
        end
      end
      spanguess=totalcores unless spanguess

      begin
        taskattrs=task.attributes
        if not taskattrs[:shared].nil? and taskattrs[:shared]
          cmd += " -n #{totalcores}"
          if totalcores>1
            cmd += " '-R span[ptile=#{spanguess}]'"
          end
          if not memoryoption.nil?
            cmd += " -R 'rusage[mem=#{memoryoption}]'"
          else
            cmd += " -R 'rusage[mem=2000]'"
          end
        else
          coresize=nodes.to_i*nodesize.to_i
          cmd += " -extsched 'CRAYLINUX[]' -R '1*{select[craylinux && !vnode]} + #{coresize}*{select[craylinux && vnode]span[ptile=#{nodesize}] cu[type=cabinet]}'"
          if not memoryoption.nil?
            cmd += " -M #{memoryoption}"
          else
            cmd += " -M 2000"
          end
        end
      rescue Exception => e
        $stderr.puts "#{e}"
        raise
      end
      
      inl=0
      task.each_native do |native_line|
        cmd += " #{native_line}"
        inl+=1
      end

      # Add the command to submit
      cmd += " #{rocotodir}/sbin/lsfcraywrapper.sh #{task.attributes[:command]}"

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
    # final_update_record: given a record from within
    #    run_bhist_bjobs, detects jobs that are reported
    #    as running, but are not actually running.
    #
    #####################################################
    def final_update_record(record,jobacct)
      if record[:state].nil?
        #WorkflowMgr.stderr("#{record[:jobid]}: nil state; return",1)
      elsif record[:state]!='RUNNING'
        #WorkflowMgr.stderr("#{record[:jobid]}: state #{record[:state]}; return",1)
      elsif record[:extsched].nil?
       # WorkflowMgr.stderr("#{record[:jobid]}: running with no extsched; return",1)
      elsif not record[:extsched].include? 'CRAYLINUX'
        #WorkflowMgr.stderr("#{record[:jobid]}: extsched=\"#{record[:extsched]}\"",1)
      elsif record[:reservation_id].nil? or record[:reservation_id]==''
        #WorkflowMgr.stderr("#{record[:jobid]}: (#{record[:jobname]} #{record[:state]} ) RUNNING, CRAYLINUX but no reservation.  Is actually queued.",1)
        record[:state]='QUEUED'
        record[:native_state]='QUEUED'
        record.delete(:start_time)
        jobacct[record[:jobid]]=record
        #WorkflowMgr.stderr("#{record[:jobid]}: override #{record[:state]} #{record[:native_state]}")
      end
      #WorkflowMgr.stderr("#{record[:jobid]}: Final state: #{record[:state]} (#{record[:native_state]})",1)
      return record
    end

  end
end
