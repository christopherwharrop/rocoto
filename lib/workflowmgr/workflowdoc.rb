##########################################
#
# Module WorkflowMgr
#
##########################################
module WorkflowMgr

  ########################################## 
  #
  # Class WorkflowXMLDoc 
  #
  ##########################################
  class WorkflowXMLDoc

    require 'libxml-ruby/libxml'
    require 'workflowmgr/utilities'
    require 'workflowmgr/cycledef'
    require 'workflowmgr/workflowlog'
    require 'workflowmgr/cycledef'
    require 'workflowmgr/sgebatchsystem'
    require 'workflowmgr/moabbatchsystem'
    require 'workflowmgr/moabtorquebatchsystem'
    require 'workflowmgr/torquebatchsystem'
    require 'workflowmgr/lsfbatchsystem'    
    require 'workflowmgr/task'


    ##########################################
    #
    # initialize
    #
    ##########################################
    def initialize(workflowdoc,workflowIOServer)

      # Set the workflowIOServer
      @workflowIOServer=workflowIOServer

      # Get the text from the xml file and put it into a string.
      # We have to do the full parsing in @workflowIOServer 
      # because we must ensure all external entities (i.e. files) 
      # are referenced inside the @workflowIOServer process and 
      # not locally.
      #
      # Update:  The above doesn't work properly because the resulting
      # XML string does not include the linefeeds in the XML header.
      # That, in turn, causes XML validation error messages to contain
      # an incorrect line number.  Therefore, existence of the top-level
      # document is checked, and then the file is parsed (including possible
      # external entities on other filesystems) outside the IO server
      # process.
      begin
        if @workflowIOServer.exists?(workflowdoc)
#          xmlstring=@workflowIOServer.parseXMLFile(workflowdoc)
          @workflowdoc = LibXML::XML::Parser.file(workflowdoc,:options => LibXML::XML::Parser::Options::NOENT).parse
        else
          raise "Cannot read XML file, #{workflowdoc}, because it does not exist!"
        end
      rescue WorkflowIOHang     
        WorkflowMgr.log("#{$!}")
        WorkflowMgr.stderr("#{$!}",1)
        raise "ERROR! Cannot read file, #{workflowdoc}, because it resides on an unresponsive filesystem"
      end

      # Parse the workflow xml string, set option to replace entities
 #     @workflowdoc=LibXML::XML::Parser.string(xmlstring,:options => LibXML::XML::Parser::Options::NOENT).parse

      # Validate the workflow xml document before metatask expansion
      validate_with_metatasks(@workflowdoc)

      # Expand metatasks
      expand_metatasks

      # Expand metatask dependencies
      expand_metataskdeps

      # Validate the workflow xml document after metatask expansion
      # The second validation is needed in case metatask expansion introduced invalid XML
      validate_without_metatasks(@workflowdoc)

    end  # initialize


    ##########################################
    #
    # realtime?
    # 
    ##########################################
    def realtime?

      if @workflowdoc.root["realtime"].nil?
        return nil
      else
        return !(@workflowdoc.root["realtime"].downcase =~ /^t|true$/).nil?
      end

    end


    ##########################################
    #
    # cyclelifespan
    # 
    ##########################################
    def cyclelifespan

      if @workflowdoc.root["cyclelifespan"].nil?
        return nil
      else
        return WorkflowMgr.ddhhmmss_to_seconds(@workflowdoc.root["cyclelifespan"])
      end

    end


    ##########################################
    #
    # cyclethrottle
    # 
    ##########################################
    def cyclethrottle

      if @workflowdoc.root["cyclethrottle"].nil?
        return nil
      else
        return @workflowdoc.root["cyclethrottle"].to_i
      end

    end


    ##########################################
    #
    # taskthrottle
    # 
    ##########################################
    def taskthrottle

      if @workflowdoc.root["taskthrottle"].nil?
        return nil
      else
        return @workflowdoc.root["taskthrottle"].to_i
      end

    end


    ##########################################
    #
    # metatask_throttles
    # 
    ##########################################
    def metatask_throttles

      return @metatask_throttles

    end


    ##########################################
    #
    # corethrottle
    # 
    ##########################################
    def corethrottle

      if @workflowdoc.root["corethrottle"].nil?
        return nil
      else
        return @workflowdoc.root["corethrottle"].to_i
      end

    end


    ##########################################
    #
    # scheduler
    # 
    ##########################################
    def scheduler

      if @workflowdoc.root["scheduler"].nil?
        return nil
      else
        return WorkflowMgr::const_get("#{@workflowdoc.root["scheduler"].upcase}BatchSystem").new
      end

    end


    ##########################################
    #
    # log
    # 
    ##########################################
    def log
 
      lognode=@workflowdoc.find('/workflow/log').first
      path=get_compound_time_string(lognode)
      verbosity=lognode.attributes['verbosity']
      verbosity=verbosity.to_i unless verbosity.nil?

      logsearch=nil
      GC.start

      return WorkflowLog.new(path,verbosity,@workflowIOServer)

    end


    ##########################################
    #
    # cycledefs
    # 
    ##########################################
    def cycledefs
 
      cycles=[]
      cyclenodes=@workflowdoc.find('/workflow/cycledef')
      cyclenodes.each { |cyclenode|
        cyclefields=cyclenode.content.strip
        nfields=cyclefields.split.size
        group=cyclenode.attributes['group']
        if nfields==3
          cycles << CycleInterval.new(cyclefields,group)
        elsif nfields==6
          cycles << CycleCron.new(cyclefields,group)
        else
	  raise "ERROR: Unsupported <cycle> type!"
        end
      }

      cyclenodes=nil
      GC.start

      return cycles

    end


    ##########################################
    #
    # tasks
    # 
    ##########################################
    def tasks

      tasks={}
      tasknodes=@workflowdoc.find('/workflow/task')
      tasknodes.each_with_index do |tasknode,seq|

        taskattrs={}
        taskenvars={}
        taskdep=nil
        taskhangdep=nil

        # Get task attributes insde the <task> tag
        tasknode.attributes.each do |attr|
          attrkey=attr.name.to_sym
          case attrkey
            when :maxtries,:throttle    # Attributes with integer values go here
              attrval=attr.value.to_i
            else                        # Attributes with string values
              attrval=attr.value
          end
          taskattrs[attrkey]=attrval
        end

        # Get task attributes, envars, and dependencies declared as elements inside <task> element
        tasknode.each_element do |e|          
          case e.name
            when /^envar$/
              envar_name=nil
              envar_value=nil
              e.each_element do |element|
                case element.name
                  when /^name$/
                    envar_name=get_compound_time_string(element)
                  when /^value$/
                    envar_value=get_compound_time_string(element)
                end
              end
              taskenvars[envar_name] = envar_value
            when /^dependency$/
              e.each_element do |element| 
                raise "ERROR: <dependency> tag contains too many elements" unless taskdep.nil?
                taskdep=Dependency.new(get_dependency_node(element))
              end
            when /^hangdependency$/
              e.each_element do |element| 
                raise "ERROR: <hangdependency> tag contains too many elements" unless taskhangdep.nil?
                taskhangdep=Dependency.new(get_dependency_node(element))
              end
            else
              attrkey=e.name.to_sym
              case attrkey
                when :cores                      # <task> elements with integer values go here
                  attrval=e.content.to_i
                when :nodes
                  attrval=e.content
                  taskattrs[:cores]=0
                  attrval.split("+").each { |nodespec|
                    resources=nodespec.split(":")
                    nodes=resources.shift.to_i
                    tpp=1
                    ppn=0
                    resources.each { |resource|
                      case resource
                        when /ppn=(\d+)/
                          ppn=$1.to_i
                        when /tpp=(\d+)/
                          tpp=$1.to_i
                      end
                    }
                    raise "ERROR: <node> tag must contain a :ppn setting for each nodespec" if ppn==0
                    taskattrs[:cores]+=nodes * ppn * tpp
                  }
                else                             # <task> elements with compoundtimestring values
                  attrval=get_compound_time_string(e)
              end
              taskattrs[attrkey]=attrval
          end
        end

        task = Task.new(seq,taskattrs,taskenvars,taskdep,taskhangdep)
        tasks[task.attributes[:name]]=task

      end

      tasknodes=nil
      GC.start

      return tasks

    end


    ##########################################
    #
    # taskdep_cycle_offsets
    # 
    ##########################################
    def taskdep_cycle_offsets

      offsets=[]
      taskdepnodes=@workflowdoc.find('//taskdep')
      taskdepnodes.each do |taskdepnode|
        offsets << WorkflowMgr.ddhhmmss_to_seconds(taskdepnode["cycle_offset"]) unless taskdepnode["cycle_offset"].nil?
      end

      taskdepnodes=nil
      GC.start

      return offsets.uniq  

    end

  private


     ##########################################
     #
     # get_compound_time_string
     # 
     ##########################################
     def get_compound_time_string(element)

       if element.nil?
         return nil
       end

       strarray=[] 
       element.each do |e|
         if e.node_type==LibXML::XML::Node::TEXT_NODE
           strarray << e.content
         else
           offset_sec=WorkflowMgr.ddhhmmss_to_seconds(e.attributes["offset"])
           case e.name
             when "cyclestr"
               formatstr=e.content
               formatstr.gsub!(/%/,'%%')
               formatstr.gsub!(/@(\^?[^@\s])/,'%\1')
               formatstr.gsub!(/@@/,'@')
               strarray << CycleString.new(formatstr,offset_sec)
             else
               raise "Invalid tag <#{e.name}> inside #{element}: #{e.node_type_name}"
           end
         end
       end

       return CompoundTimeString.new(strarray)

     end


     ##########################################
     #
     # get_dependency_node
     # 
     ##########################################
     def get_dependency_node(element)
 
       # Build a dependency tree
       children=[]
       element.each_element { |e| children << e }
       case element.name
         when "not"
           return Dependency_NOT_Operator.new(children.collect { |child| get_dependency_node(child) })
         when "and"
           return Dependency_AND_Operator.new(children.collect { |child| get_dependency_node(child) })
         when "or"
           return Dependency_OR_Operator.new(children.collect { |child|  get_dependency_node(child) })
         when "nand"
           return Dependency_NAND_Operator.new(children.collect { |child|  get_dependency_node(child) })
         when "nor"
           return Dependency_NOR_Operator.new(children.collect { |child|  get_dependency_node(child) })
         when "xor"
           return Dependency_XOR_Operator.new(children.collect { |child|  get_dependency_node(child) })
         when "some"
           return Dependency_SOME_Operator.new(children.collect { |child|  get_dependency_node(child) }, element["threshold"].to_f)
         when "taskdep"
           return get_taskdep(element)
         when "datadep"
           return get_datadep(element)
         when "timedep"
           return get_timedep(element)
       end

     end


     #####################################################
     #
     # get_taskdep
     #
     #####################################################
     def get_taskdep(element)
 
       # Get the mandatory task attribute
       task=element.attributes["task"]
 
       # Get the state attribute
       state=element.attributes["state"] || "SUCCEEDED"
 
       # Get the cycle offset, if there is one
       cycle_offset=WorkflowMgr.ddhhmmss_to_seconds(element.attributes["cycle_offset"]) || 0
 
       return TaskDependency.new(task,state,cycle_offset)

     end


     ##########################################
     # 
     # get_datadep
     # 
     ##########################################
     def get_datadep(element)
 
       # Get the age attribute
       age_sec=WorkflowMgr.ddhhmmss_to_seconds(element.attributes["age"]) || 0

       # Get the minsize attribute
       minsize=element.attributes["minsize"] || 0
       case minsize
         when /^(\d+)$/
           minsize=$1.to_i
         when /^(\d+)[B|b]$/
           minsize=$1.to_i
         when /^(\d+)[K|k]$/
           minsize=$1.to_i * 1024
         when /^(\d+)[M|m]$/
           minsize=$1.to_i * 1024 * 1024
         when /^(\d+)[G|g]$/
           minsize=$1.to_i * 1024 * 1024 * 1024
       end

       return DataDependency.new(get_compound_time_string(element),age_sec,minsize)
 
     end
 

     #####################################################
     #
     # get_timedep
     #
     #####################################################
     def get_timedep(element)
 
       # Get the time cycle string
       return TimeDependency.new(get_compound_time_string(element))
 
     end


    ##########################################
    #
    # validate_with_metatasks
    # 
    ##########################################
    def validate_with_metatasks(doc)

      # Parse the Relax NG schema XML document
      xmlstring=@workflowIOServer.parseXMLFile("#{File.dirname(__FILE__)}/schema_with_metatasks.rng")
      relaxng_document=LibXML::XML::Parser.string(xmlstring,:options => LibXML::XML::Parser::Options::NOENT).parse

      # Prepare the Relax NG schemas for validation
      relaxng_schema = LibXML::XML::RelaxNG.document(relaxng_document)

      # Validate the workflow XML file against the general Relax NG Schema that validates metatask tags
      doc.validate_relaxng(relaxng_schema)

    end


    ##########################################
    #
    # validate_without_metatasks
    #
    ##########################################
    def validate_without_metatasks(doc)

      # Parse the Relax NG schema XML document
      xmlstring=@workflowIOServer.parseXMLFile("#{File.dirname(__FILE__)}/schema_without_metatasks.rng")
      relaxng_document=LibXML::XML::Parser.string(xmlstring,:options => LibXML::XML::Parser::Options::NOENT).parse

      # Prepare the Relax NG schemas for validation
      relaxng_schema = LibXML::XML::RelaxNG.document(relaxng_document)

      # Validate the workflow XML file against the general Relax NG Schema that validates metatask tags
      doc.validate_relaxng(relaxng_schema)

    end

    ##########################################
    #
    # expand_metataskdeps
    #
    ##########################################
    def expand_metataskdeps

      @workflowdoc.root.each_element {|ch|
        if ch.name == "task"
          # Find the metataskdep nodes
          metataskdeps=ch.find('.//metataskdep')

          # Replace each of them with the equivalient <and><taskdep/><taskdep/>...</and> expression
          metataskelements=[]
          metataskdeps.each { |metataskdep|

            metataskelements << metataskdep

            # Get the name of the metatask
            metatask=metataskdep["metatask"]

            # Find all names of tasks descended from the metatask
            tasknames=[]
            tasks=@workflowdoc.find("//task[contains(@metatasks,'#{metatask}')]")
            tasks.each { |task| tasknames << task["name"] if task["metatasks"]=~/^([^,]+,)*#{metatask}(,[^,]+)*$/ }

            # Insert a "some" element after the metataskdep element
            somenode=LibXML::XML::Node.new("some")
            somenode["threshold"] = metataskdep["threshold"].nil? ? "1.0" : metataskdep["threshold"]
            metataskdep.next=somenode

            # Add taskdep elements as children to the and element
            tasknames.each do |task|
              taskdepnode=LibXML::XML::Node.new("taskdep")
              taskdepnode["task"]=task
              taskdepnode["cycle_offset"]=metataskdep["cycle_offset"] unless metataskdep["cycle_offset"].nil?
              taskdepnode["state"]=metataskdep["state"] unless metataskdep["state"].nil?
              somenode << taskdepnode
            end

          }

          # Remove the metataskdep elements
          metataskelements.each { |metataskelement| metataskelement.remove! }

        end
      }    

    end


    ##########################################
    #
    # expand_metatasks
    #
    ##########################################
    def expand_metatasks

      # Parse and expand metatasks
      metatasks=[]
      @metatask_seq=1
      @metatask_throttles={}
      @workflowdoc.root.each_element {|ch|
        if ch.name == "metatask"
          if ch["name"].nil?
            ch["name"]="metatask#{@metatask_seq}"
            @metatask_seq+=1
          end
          metatask_name=ch["name"]
          @metatask_throttles[metatask_name]=ch["throttle"].nil? ? 999999 : ch["throttle"].to_i
	  pre_parse(ch,metatask_name)
          metatasks << ch
        end
      }
      metatasks.each {|ch| ch.remove!}

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
    def pre_parse(metatask,metatask_list)
    
      id_table = {}
      var_length = -1

      # Set the metatask list for all task children of this metatask
      metatask.children.each{|e|
        if e.name == "task"
          e["metatasks"]=metatask_list
        end
      }

      # Parse each metatask child of this metatask, adding the metatask child to the metatask list
      metatask.children.each {|ch|
        if ch.name == "metatask"
          if ch["name"].nil?
            ch["name"]="metatask#{@metatask_seq}"
            @metatask_seq+=1
          end
          metatask_name=ch["name"]
          metatask_list += ",#{metatask_name}"
          @metatask_throttles[metatask_name]=ch["throttle"].nil? ? 999999 : ch["throttle"].to_i
          pre_parse(ch,metatask_list)
        end
      }

      # Build a table of var tags and their values for this metatask
      metatask.children.each {|e|
        if e.name == "var"  
          var_values = e.content.split
          var_length = var_values.length if var_length == -1
          raise "ERROR: <var> tags do not contain the same number of items!" if var_values.length != var_length
          id_table[e["name"]] = var_values
        end
      }
      raise "ERROR: No <var> tag or values specified in one or more metatasks" if var_length < 1

      # Expand the metatasks, adding metatask list only to the expanded tasks from nested metatasks
      task_list = Array.new
      0.upto(var_length - 1) {|index|
        metatask.children.each{|e|
          if e.name == "task"
            task_copy = e.copy(true)
            if task_copy["metatasks"].nil?
              task_copy["metatasks"]=metatask_list
            end
            traverse(task_copy,id_table, index)
            task_list << task_copy
          end
        }
      }

       # Insert the expanded tasks into the XML tree
      (task_list.length - 1).downto(0) {|x| metatask.next = task_list[x]}

    end

  end  # Class WorkflowXMLDoc

end  # Module WorkflowMgr
