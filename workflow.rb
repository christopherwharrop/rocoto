unless defined? $__workflow__

##########################################
#
# Class Workflow
#
##########################################
class Workflow

  require 'libxml.rb'
  require 'pstore'
  require 'workflowlog.rb'
  require 'cyclecron.rb'
  require 'sgebatchsystem.rb'
  require 'loadlevelerbatchsystem.rb'
  require 'lsfbatchsystem.rb'
  require 'task.rb'
  require 'property.rb'
  require 'environment.rb'
  require 'dependency.rb'
  require 'cyclestring.rb'
  require 'lockfile/lib/lockfile.rb'

  class XMLError < StandardError; end

  @@update_interval=60
  @@opts={ 
          :retries => 1,
          :sleep_inc => 2,
          :min_sleep => 2, 
          :max_sleep => 10,
          :max_age => 900,
          :suspend => 30,
          :refresh => 5,
          :timeout => 45,
          :poll_retries => 16,
          :poll_max_sleep => 0.08,
          :debug => false
  }
  @@ctrl_opts={ 
          :retries => nil,
          :sleep_inc => 2,
          :min_sleep => 2, 
          :max_sleep => 10,
          :max_age => 900,
          :suspend => 30,
          :refresh => 5,
          :timeout => 45,
          :poll_retries => 16,
          :poll_max_sleep => 0.08,
          :debug => false
  }

  attr_reader :state
 
  #####################################################
  #
  # initialize
  #
  #####################################################
  def initialize(xmlfile,store,opts=@@opts)

    @store=PStore.new(store)
    @lockfile="#{store}.lock"

    begin
#      Lockfile.new(@lockfile,opts) do

        # Retrieve the previous XML filename and parse time
        @store.transaction do
          @xmlfile=@store['XMLFILE']
          @xmlparsetime=@store['XMLPARSETIME']
          @realtime=@store['REALTIME']
          @log=@store['LOG']
          @cyclecrons=@store['CYCLECRONS'].nil?   ? Hash.new : @store['CYCLECRONS']
          @cycles=@store['CYCLES'].nil?           ? Hash.new : @store['CYCLES']
          @maxflowrate=@store['MAXFLOWRATE'].nil? ? 0.0      : @store['MAXFLOWRATE']
          @tasks=@store['TASKS'].nil?             ? Hash.new : @store['TASKS']
          @taskorder=@store['TASKORDER'].nil?     ? Hash.new : @store['TASKORDER']
          @schedulers=@store['SCHEDULERS'].nil?   ? Hash.new : @store['SCHEDULERS']
          @status=@store['STATUS'].nil?           ? Hash.new : @store['STATUS']
          @reservation_lead_time=@store['RESERVATION_LEAD_TIME'].nil? ? 0 : @store['RESERVATION_LEAD_TIME']
        end

        # For backward compatibility where store files use a string instead of a Hash for @status
        # This code should be removed at some point
        unless @status.class==Hash
          @status=Hash.new
          @cycles.keys.each { |cycle| @status[cycle]="RUN" }
        end 

        # Update @xmlfile and @xmlparsetime if the xml filename has changed
        if @xmlfile != xmlfile
          @xmlfile=xmlfile
          @xmlparsetime=nil
        end

        # If the XML file was modified or has changed...
        if self.dirty? 

          # Parse the XML file
          self.parseXML

          # Save updated workflow to store file
          @store.transaction do
            @store['XMLFILE']=@xmlfile
            @store['XMLPARSETIME']=@xmlparsetime
            @store['REALTIME']=@realtime
            @store['LOG']=@log
            @store['CYCLECRONS']=@cyclecrons
            @store['CYCLES']=@cycles
            @store['MAXFLOWRATE']=@maxflowrate
            @store['TASKS']=@tasks
            @store['TASKORDER']=@taskorder
            @store['SCHEDULERS']=@schedulers
            @store['STATUS']=@status
            @store['RESERVATION_LEAD_TIME']=@reservation_lead_time
          end

        end

#      end

    rescue Lockfile::MaxTriesLockError
      puts "The workflow is locked."
      raise
    rescue ArgumentError
      if $!.message=~/marshal data too short/
        puts
        puts "WARNING!  Store file corruption has been detected."
        puts
        self.parseXML
        if @realtime
          puts "Attempting to automatically recover by removing the store file, #{store}."
          puts "All workflow history and state has been lost."
          File.delete(store)
        else
          puts "Manual recovery is required.  First, adjust the cycle specification in the XML file"
          puts "to eliminate cycles that have already completed successfully.  Then, remove the"
          puts "corrupted store file, #{store}.  The workflow should then resume."
        end
        puts
      end
      raise
    end

  end


  #####################################################
  #
  # dirty?
  #
  #####################################################
  def dirty?

    return @xmlparsetime.nil? || File.mtime(@xmlfile) > @xmlparsetime

  end


  #####################################################
  #
  # pexit
  #
  #####################################################

  def pexit(msg)
    puts msg
    exit!
  end

  #####################################################
  #
  # traverse
  #
  #####################################################

  def traverse(node, id_table, index)
    
    if node.node_type_name == "text"
      cont = node.content
      id_table.each{|id, value|
	next while cont.sub!("#"+id+"#", id_table[id][index])
      }
      node.content = cont
      
    else
      node.attributes.each{|attr|
	val = attr.value
	id_table.each{|id, value|
	  next while val.sub!("#"+id+"#", id_table[id][index])
	}
        attr.value = val
      }
      node.children.each{|ch| traverse(ch, id_table, index)}
    end

  end

  
  #####################################################
  #
  # pre-parse
  #
  #####################################################
  
  def pre_parse(metatask)
    
    id_table = {}
    var_length = -1

    metatask.children.each {|ch|
      pre_parse(ch) if ch.name == "metatask"
    }

    metatask.children.each {|e|
      if e.name == "var"
	var_values = e.content.split
        var_length = var_values.length if var_length == -1
        pexit("unequal variables") if var_values.length != var_length
        id_table[e["id"]] = var_values

#	eval "#{e["id"]} = e.content.split"
#        var_length = eval "#{e["id"]}.length" if var_length == -1
#        pexit("unequal variables") if (eval "#{e["id"]}.length") != var_length
#        id_table[e["id"]] = eval "#{e['id']}"
      end
    }
    pexit("no <var> tag or values specified in one or more metatasks") if var_length < 1

    task_list = Array.new
    0.upto(var_length - 1) {|index|
      metatask.children.each{|e|
        if e.name == "task"
          task_copy = e.copy("deep")
          traverse(task_copy,id_table, index)
          task_list << task_copy
        end
      }
    }

    (task_list.length - 1).downto(0) {|x| metatask.next = task_list[x]}

  end

  #####################################################
  #
  # parseXML
  #
  #####################################################
  def parseXML

    # Set the parsetime
    @xmlparsetime=Time.now

    # Parse the Relax NG schemas as XML document ("_m" means it validates metatask tags)
    relaxng_document_m = LibXML::XML::Document.file("#{File.dirname(__FILE__)}/schema_m.rng")
    relaxng_document = LibXML::XML::Document.file("#{File.dirname(__FILE__)}/schema.rng")

    # Prepare the Relax NG schemas for validation
    relaxng_schema_m = LibXML::XML::RelaxNG.document(relaxng_document_m)
    relaxng_schema = LibXML::XML::RelaxNG.document(relaxng_document)

    # Parse the workflow xml document
    workflowdoc=LibXML::XML::Document.file(@xmlfile,:options => LibXML::XML::Parser::Options::NOENT)

    # Validate the workflow XML file against the general Relax NG Schema that validates metatask tags
    workflowdoc.validate_relaxng(relaxng_schema_m)

    # Parse and expand metatasks
    workflow=workflowdoc.root
    workflow.children.each {|ch|
      if ch.name == "metatask"
	pre_parse(ch)
        ch.remove!
      end
    }

    # Validate the workflow XML file again against the Relax NG Schema that does not include metatasks
    # This helps to verify the correctness ater/of the metatask expansion
    workflowdoc.validate_relaxng(relaxng_schema)

    # Get the workflow realtime attribute
    case workflow.attributes["realtime"]
      when /^true$/i,/^t$/i
        @realtime=true
      when /^false$/i,/^f$/i
        @realtime=false
      when nil
        @realtime=false
      else
        raise XMLError,"<workflow> attribute 'realtime' contains illegal value: '#{workflow.attributes["realtime"]}'"
    end

    # Get the workflow flowrate attribute
    case workflow.attributes["maxflowrate"]
      when /^\d+[.]{0,1}\d*$/,/^[.]{0,1}\d*$/
        @maxflowrate=workflow.attributes["maxflowrate"].to_f
      when nil,"0"
        @maxflowrate=nil
      else
        raise XMLError,"<workflow> attribute 'maxflowrate' contains illegal value: '#{workflow.attributes["maxflowrate"]}'"
    end

    # Get the workflow reservation_lead_time attribute
    case workflow.attributes["reservation_lead_time"]
      when /^\d+$/
        @reservation_lead_time=workflow.attributes["reservation_lead_time"].to_i
      when nil,"0"
        @reservation_lead_time=0
      else
        raise XMLError,"<workflow> attribute 'reservation_lead_time' contains illegal value: '#{workflow.attributes["reservation_lead_time"]}'"
    end

    # Parse log, cycle, and task tags in depth-first order
    @log=nil
    @cyclecrons.clear
    @schedulers.clear
    @taskorder.clear
    workflow.each {|e|
      next unless e.node_type==LibXML::XML::Node::ELEMENT_NODE
      case e.name
        when "log"
          if @log.nil?
            self.parse_log(e)
          else
            raise XMLError, "Multiple <log> tags inside a <workflow> is not allowed"     
          end
        when "cycle"
          self.parse_cycle(e)
        when "task"
          self.parse_task(e)
        else
          raise "ERROR: Invalid tag, <#{e.name}>"
      end 
    }

    # Remove tasks that are no longer in the XML file
    @tasks.delete_if { |taskid,task|
      @taskorder[taskid].nil?
    }

  end

  #####################################################
  #
  # parse_log
  #
  #####################################################
  def parse_log(element)

     # Make sure element corresponds to a <log> tag
     unless element.respond_to?(:name)
       raise XMLError,"Invalid argument. Element '#{element}' is not a <log> element"
     end
     unless element.name=="log"
       raise XMLError,"Invalid argument. Element '#{element}' is not a <log> element"
     end

     # Create and set the WorkflowLog
     @log=WorkflowLog.new(CycleString.new(element))

  end

  #####################################################
  #
  # parse_cycle
  #
  #####################################################
  def parse_cycle(element)

     # Make sure element corresponds to a <cycle> tag
     unless element.respond_to?(:name)
       raise XMLError,"Invalid argument. Element '#{element}' is not a <cycle> element"
     end
     unless element.name=="cycle"
       raise XMLError,"Invalid argument. Element '#{element}' is not a <cycle> element"
     end
 
     # Get the fields
     year,month,day,hour,min=element.content.strip.split(/\s+/)
 
     # Make sure year field is not a "*"
     raise XMLError,"The year field of a <cycle> can not be a '*'" if year=="*"

     # Get the cycle id
     id=element.attributes["id"]
     if id.nil?
       id=(@cyclecrons.length+1).to_s
     end

     # Make sure the id is unique
     raise XMLError,"Duplicate definition of <cycle> with id='#{id}'" unless @cyclecrons[id].nil?

     # Create a new cycle and add it to the hash of cyclecrons
     @cyclecrons[id]=CycleCron.new(id,year,month,day,hour,min)

  end


  #####################################################
  #
  # parse_task
  #
  #####################################################
  def parse_task(element)

    # Make sure element corresponds to a <task> tag
    unless element.respond_to?(:name)
      raise XMLError,"Invalid argument. Element '#{element}' is not a <task> element"
    end
    unless element.name=="task"
      raise XMLError,"Invalid argument. Element '#{element}' is not a <task> element"
    end

    # Get the task id attribute
    taskid=element.attributes["id"]
    raise XMLError,"<task> is missing the mandatory 'id' attribute" if taskid.nil?
    raise XMLError,"Duplicate definition of <task> with id='#{taskid}'" unless @taskorder[taskid].nil?

    # Get the task action attribute
    taskaction=element.attributes["action"]
    raise XMLError,"<task> is missing the mandatory 'action' attribute" if taskaction.nil?

    # Get the task sched attribute
    scheduler=element.attributes["scheduler"]
    case scheduler
      when /^sge$/i
        @schedulers[scheduler.upcase]=SGEBatchSystem.new() if @schedulers[scheduler.upcase].nil?
      when /^ll$/i
        @schedulers[scheduler.upcase]=LoadLevelerBatchSystem.new() if @schedulers[scheduler.upcase].nil?
      when /^lsf$/i
        @schedulers[scheduler.upcase]=LSFBatchSystem.new() if @schedulers[scheduler.upcase].nil?
      when nil
        raise XMLError,"<task> is missing the mandatory 'scheduler' attribute" 
      else
        raise XMLError,"<task> attribute 'scheduler' refers to an unrecognized scheduler type: '#{scheduler}'" 
    end
    tasksched=@schedulers[scheduler.upcase]

    # Get the task tries attribute
    tries=element.attributes["tries"]
    case tries
      when /^[0-9]+$/
        tasktries=tries.to_i
      when nil
        tasktries=0
      else 
        raise XMLError,"<task> attribute 'tries' must be a non-negative integer"
    end

    # Get the task throttle attribute
    throttle=element.attributes["throttle"]
    case throttle
      when /^[0-9]+$/
        taskthrottle=throttle.to_i
      when nil
        taskthrottle=0
      else 
        raise XMLError,"<task> attribute 'throttle' must be a non-negative integer"
    end

    # Get the cycle attribute
    cycleids=element.attributes["cycle"]
    case cycleids
      when /^\*$/, /^all$/i, nil
        taskcycles=@cyclecrons.values
      else
        taskcycles=Array.new
        cycleids.split(",").each { |cycleid|
          unless @cyclecrons.has_key?(cycleid)
            raise XMLError,"<task> attribute 'cycle' refers to a cycle (#{cycleid}) that has not been defined"
          end
          taskcycles.push(@cyclecrons[cycleid])
        }
    end

    taskproperties=Hash.new
    taskenvironments={"__WORKFLOWMGR__"=>Environment.new("__WORKFLOWMGR__",CycleString.new(@xmlfile))}
    taskdependency=nil
    taskhangdependency=nil
    taskdeadlinedependency=nil
    element.each { |e|

      next unless e.node_type==LibXML::XML::Node::ELEMENT_NODE

      case e.name
        when "property"
          property=parse_property(e)
          taskproperties[property.name]=property
        when "environment"
          environment=parse_environment(e)
          taskenvironments[environment.name]=environment
        when "dependency"
          elements=e.find_all {|node| node.node_type==LibXML::XML::Node::ELEMENT_NODE}
          raise XMLError,"<dependency> tag can contain only one tag" if elements.size > 1
          taskdependency=Dependency.new(parse_dependency_node(elements[0]))
        when "hangdependency"
          elements=e.find_all {|node| node.node_type==LibXML::XML::Node::ELEMENT_NODE}
          raise XMLError,"<hangdependency> tag can contain only one tag" if elements.size > 1
          taskhangdependency=Dependency.new(parse_dependency_node(elements[0]))
        when "deadlinedependency"
          elements=e.find_all {|node| node.node_type==LibXML::XML::Node::ELEMENT_NODE}
          raise XMLError,"<deadlinedependency> tag can contain only one tag" if elements.size > 1
          taskdeadlinedependency=Dependency.new(parse_dependency_node(elements[0]))
        else
          raise XMLError,"Invalid tag, <#{e.name}>"
      end

    }

    # Create task object or update one that we had from the last time we parsed
    if @tasks[taskid].nil?
      @tasks[taskid]=Task.new(taskid,taskaction,tasksched,taskcycles,tasktries,taskthrottle,taskproperties,taskenvironments,taskdependency,taskhangdependency,taskdeadlinedependency,@log)
    else
      @tasks[taskid].alter(taskid,taskaction,tasksched,taskcycles,tasktries,taskthrottle,taskproperties,taskenvironments,taskdependency,taskhangdependency,taskdeadlinedependency,@log)
    end

    # Set the sequence number for this task
    @taskorder[taskid]=@taskorder.length+1

  end


  #####################################################
  #
  # parse_property
  #
  #####################################################
  def parse_property(element)

    name=nil
    value=nil
   
    # Get the list of elements
    element.each { |e|

      next unless e.node_type==LibXML::XML::Node::ELEMENT_NODE

      case e.name
        when "name"
          raise XMLError,"<property> must contain exactly one <name>" unless name.nil?
          name=e.content.strip
        when "value"
          raise XMLError,"<property> may not contain more than one <value>" unless value.nil?
          value=CycleString.new(e)
        else
      end   
    }

    raise XMLError,"<property> must contain exactly one <name>" if name.nil?

    return Property.new(name,value)

  end


  #####################################################
  #
  # parse_environment
  #
  #####################################################
  def parse_environment(element)

    name=nil
    value=nil
   
    # Get the list of elements
    element.each { |e|

      next unless e.node_type==LibXML::XML::Node::ELEMENT_NODE

      case e.name
        when "name"
          raise XMLError,"<environment> must contain exactly one <name>" unless name.nil?
          name=e.content.strip
        when "value"
          raise XMLError,"<environment> may not contain more than one <value>" unless value.nil?
          value=CycleString.new(e)
        else
      end   
    }

    raise XMLError,"<environment> must contain exactly one <name>" if name.nil?

    return Environment.new(name,value)

  end


  #####################################################
  #
  # parse_dependency_node
  #
  #####################################################
  def parse_dependency_node(element)

    # Build a dependency tree
    elements=element.find_all {|node| node.node_type==LibXML::XML::Node::ELEMENT_NODE}
    case element.name
      when "not"
        raise XMLError,"<not> tags can contain only one tag" if elements.size > 1
        return Dependency_NOT_Operator.new(elements.collect { |child|
          parse_dependency_node(child)
        })          
      when "and"
        if element.attributes['max_missing'].nil?
          max_missing=0
        else
          max_missing=element.attributes['max_missing'].to_i
        end
        return Dependency_AND_Operator.new(elements.collect { |child|
          parse_dependency_node(child)
        },max_missing)          

      when "or"
        return Dependency_OR_Operator.new(elements.collect { |child|
          parse_dependency_node(child)
        })          
      when "taskdep"
        return parse_task_taskdep(element)
      when "filedep"
        return parse_task_filedep(element)
      when "timedep"
        return parse_task_timedep(element)
      else
        raise XMLError,"Invalid tag, <#{element.name}>"
    end
    
  end


  #####################################################
  #
  # parse_task_filedep
  #
  #####################################################
  def parse_task_filedep(element)

    # Get the age attribute
    age_str=element.attributes["age"]
    case age_str
      when /^[0-9:]+$/
        age_sec=0
        age_str.split(":").reverse.each_with_index {|i,index| 
          if index==3
            age_sec+=i.to_i*3600*24
          elsif index < 3
            age_sec+=i.to_i*60**index
          else
            raise "Invalid age, '#{age_str}' inside of #{e}"
          end           
        }

      when nil
        age_sec=0
      else
        raise XMLError,"<filedep> attribute 'age' must be a non-negative time given in dd:hh:mm:ss format"
    end

    return FileDependency.new(CycleString.new(element),age_sec)

  end

  #####################################################
  #
  # parse_task_taskdep
  #
  #####################################################
  def parse_task_taskdep(element)

    # Get the mandatory task attribute
    taskref=element.attributes["task"]
    raise XMLError,"<taskdep> is missing the mandatory 'task' attribute" if taskref.nil?

    # Make sure the task attribute refers to a previously defined <task> tag
    raise XMLError,"<taskdep> task attribute refers to a task, #{taskref}, that has not been previously defined" if @taskorder[taskref].nil?

    # Get the cycle attribute
    cycle=element.attributes["cycle"]
    case cycle
      when /^[+-]*[0-9]+$/
        cycleref=cycle.to_i
      when nil
        cycleref=0
      else
        raise XMLError,"<taskdep> attribute 'cycle' must be a positive or negative integer"
    end

    return TaskDoneOkayDependency.new(@tasks[taskref],cycleref)

  end

  #####################################################
  #
  # parse_task_timedep
  #
  #####################################################
  def parse_task_timedep(element)

    # Get the time cycle string
    timestr=CycleString.new(element)
    raise XMLError,"<timedep> format must be YYYYMMDDHHMMSS" unless timestr.to_s(Time.now)=~/^[0-9]{14}$/

    return TimeDependency.new(timestr)
 
  end


  #####################################################
  #
  # done?
  #
  #####################################################
  def done?

    if @realtime
      return false
    else
      return (@cycles.keys.all? { |cycle| cycle_done?(cycle)})
    end

  end


  #####################################################
  #
  # cycle_done?
  #
  #####################################################
  def cycle_done?(cycle)

    return(@tasks.values.all? { |task| task.done_okay?(cycle) } ||
           @tasks.values.any? { |task| task.crashed?(cycle) })

  end

  #####################################################
  #
  # run
  #
  #####################################################
  def run(opts=@@opts)

    begin

      # Lock the workflow while working on tasks and cycles
#      Lockfile.new(@lockfile,opts) do

        # Calculate a new cycle to add if we are doing realtime
        if @realtime

# NOTE: this doesn't work for subhourly cycles!!!! 
          # Get the latest cycle not greater than the current time
          0.step(@reservation_lead_time,3600) {|i|
            nextcycles=@cyclecrons.values.collect { |cyclecron| cyclecron.prev(Time.now+i) }.compact
            if nextcycles.empty?
              newcycle=nil
            else
              newcycle=nextcycles.max
            end

            # Add the new cycle if there is one
            unless newcycle.nil?
              unless @cycles.has_key?(newcycle)
                @cycles[newcycle]=Time.now
                @status[newcycle]="RUN"
              end
            end
          }

        # Calculate a new cycle to add if we are doing retrospective
        else

          # If the maxflowrate is undefined, then add a cycle every time we run
          if @maxflowrate.nil? || @maxflowrate <= 0
            numcycles=1
          else
            denominator=(@maxflowrate-@maxflowrate.to_i).zero? ? 1 : @maxflowrate-@maxflowrate.to_i

            # Calculate how far back to look
            lookback_hours=(1.0/denominator).round

            # Calculate the max cycles per lookback_hours
            max_cycles=(@maxflowrate/denominator).round

            # Calculate the number of cycles to add
            numcycles=max_cycles - @cycles.values.find_all { |add_time| add_time > Time.now - lookback_hours*3600 }.size

          end

          # Add numcycles cycles
          numcycles.times {
            if @cycles.empty?
              latest_cycle=Time.at(0).getgm
            else
              latest_cycle=@cycles.keys.max
            end

            # Get the earliest cycle not less than the latest cycle already added
            nextcycles=@cyclecrons.values.collect { |cyclecron| cyclecron.next(latest_cycle) }.compact
            if nextcycles.empty?
              newcycle=nil
            else
              newcycle=nextcycles.min
            end

            # Add the new cycle if it hasn't been added yet
            unless newcycle.nil?
              unless @cycles.has_key?(newcycle)
                @cycles[newcycle]=Time.now
                @status[newcycle]="RUN"
              end
            end
          }

        end

        @store.transaction do
          @store['CYCLES']=@cycles
          @store['STATUS']=@status
        end

        # Loop over cycles
        @cycles.keys.sort.each { |cycle|

          # Don't run this cycle unless the cycle status is "RUN"
          next unless @status[cycle]=="RUN"

          Debug::message("Processing cycle #{cycle}",5)

          # Run tasks in XML tree breadth-first order
          @tasks.keys.sort {|a,b| @taskorder[a]<=>@taskorder[b]}.each { |task|
            Debug::message("  Processing task #{task}",5)
            @tasks[task].run(cycle)
          }

        }

#      end

    rescue
      puts $!
      exit
    ensure
      @store.transaction do
        @store['CYCLES']=@cycles
        @store['TASKS']=@tasks
        @store['SCHEDULERS']=@scheduler
        @store['STATUS']=@status
      end
    end

  end


  #####################################################
  #
  # halt
  #
  #####################################################
  def halt(cycles,opts=@@ctrl_opts)

#    Lockfile.new(@lockfile,opts) do
      begin
        if cycles.nil?
          shutdowncycles=@cycles.keys
        else
          shutdowncycles=cycles
        end
        shutdowncycles.collect! {|cycle| cycle.getgm}
        shutdowncycles.each { |shutdowncycle|
          @log.log(shutdowncycle,"Attempting to halt this cycle")
          @tasks.each_value { |task| 
            task.halt(shutdowncycle)
          }
          @status[shutdowncycle]="HALT"
          @log.log(shutdowncycle,"This cycle has been halted")
        }
      rescue
        raise $!
      ensure
        @store.transaction do
          @store['TASKS']=@tasks
          @store['STATUS']=@status
        end        
      end      
#    end

  end


  #####################################################
  #
  # pause
  #
  #####################################################
  def pause(cycles,opts=@@ctrl_opts)
  
    Lockfile.new(@lockfile,opts) do

      @status="PAUSE"
      @store.transaction do
        @store['STATUS']=@status
      end        
    end

  end

  #####################################################
  #
  # resume
  #
  #####################################################
  def resume(cycles,opts=@@ctrl_opts)
  
    Lockfile.new(@lockfile,@@opts) do

      begin
        if cycles.nil?
          resumecycles=@cycles.keys
        else
          resumecycles=cycles
        end
        resumecycles.each { |resumecycle|

          # Make sure we don't resume a cycle that doesn't exist
          next if @cycles[resumecycle].nil?

          @log.log(resumecycle,"Cycle has been resumed")
          @status[resumecycle]="RUN"
        }
      rescue
        raise $!
      ensure
        @store.transaction do
          @store['TASKS']=@tasks
          @store['STATUS']=@status
        end        
      end      

    end

  end

end

$__workflow__ == __FILE__
end
