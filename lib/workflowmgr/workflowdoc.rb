##########################################
#
# Module WorkflowMgr
#
##########################################
require 'English'
module WorkflowMgr
  ##########################################
  #
  # Class WorkflowXMLDoc
  #
  ##########################################
  class WorkflowXMLDoc
    require 'time'

    require 'libxml'
    require 'workflowmgr/utilities'
    require 'workflowmgr/cycledef'
    require 'workflowmgr/workflowlog'
    require 'workflowmgr/cycledef'
    require 'workflowmgr/moabbatchsystem'
    require 'workflowmgr/moabtorquebatchsystem'
    require 'workflowmgr/torquebatchsystem'
    require 'workflowmgr/pbsprobatchsystem'
    require 'workflowmgr/lsfbatchsystem'
    require 'workflowmgr/lsfcraybatchsystem'
    require 'workflowmgr/slurmbatchsystem'
    require 'workflowmgr/task'

    @@messages = {
      exclusive: "Ignoring <exclusive>; this platform does not support it.",
      shared: "Ignoring <shared>; this platform does not support it."
    }

    def unescape(xmlstr)
      # This is a workaround for a LibXML bug: it is impossible to
      # disable output escaping.  That means strings will have &quot;
      # instead of " no matter what you do.  This function replaces
      # some common XML entities with their corresponding values.
      t = xmlstr.gsub(/&quot;/, '"').gsub(/&lt;/, '<').gsub(/&gt;/, '>')

      # The &amp; must be last:
      t.gsub(/&amp;/, '&')
    end

    ##########################################
    #
    # initialize
    #
    ##########################################
    def initialize(workflowdoc, workflow_io_server, config)
      # Set the configuration
      @config = config

      # Set the workflow_io_server
      @workflow_io_server = workflow_io_server

      # Track features used in the document that are only supported by
      # some platforms:
      @features_used = {}

      # Get the text from the xml file and put it into a string.
      # We have to do the full parsing in @workflow_io_server
      # because we must ensure all external entities (i.e. files)
      # are referenced inside the @workflow_io_server process and
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
        if @workflow_io_server.exist?(workflowdoc)
          context = LibXML::XML::Parser::Context.file(workflowdoc)
          context.options = LibXML::XML::Parser::Options::NOENT | LibXML::XML::Parser::Options::HUGE | LibXML::XML::Parser::Options::NOCDATA
          parser = LibXML::XML::Parser.new(context)
          @workflowdoc = parser.parse
        else
          raise "Cannot read XML file, #{workflowdoc}, because it does not exist!"
        end
      rescue WorkflowIOHang
        WorkflowMgr.log($ERROR_INFO.to_s)
        WorkflowMgr.stderr($ERROR_INFO.to_s, 2)
        raise "ERROR! Cannot read file, #{workflowdoc}, because it resides on an unresponsive filesystem"
      end

      # Validate the workflow xml document before metatask expansion
      validate_with_metatasks(@workflowdoc)
      # Expand metatasks
      expand_metatasks

      # Expand metatask dependencies
      expand_metataskdeps

      # Insert dependencies for auto-serialized metatasks
      expand_serialdeps

      # Validate the workflow xml document after metatask expansion
      # The second validation is needed in case metatask expansion introduced invalid XML
      validate_without_metatasks(@workflowdoc)
    end

    ##########################################
    #
    # realtime?
    #
    ##########################################
    def realtime?
      if @workflowdoc.root.attributes?
        realtime = @workflowdoc.root.attributes["realtime"]
        if realtime.nil?
          nil
        else
          !(realtime.downcase =~ /^t|true$/).nil?
        end
      end
    end

    ##########################################
    #
    # cyclelifespan
    #
    ##########################################
    def cyclelifespan
      if @workflowdoc.root.attributes?
        cls = @workflowdoc.root.attributes["cyclelifespan"]
        if cls.nil?
          nil
        else
          WorkflowMgr.ddhhmmss_to_seconds(cls)
        end
      end
    end

    ##########################################
    #
    # cyclethrottle
    #
    ##########################################
    def cyclethrottle
      if @workflowdoc.root.attributes?
        ct = @workflowdoc.root.attributes["cyclethrottle"]
        if ct.nil?
          nil
        else
          ct.to_i
        end
      end
    end

    ##########################################
    #
    # taskthrottle
    #
    ##########################################
    def taskthrottle
      if @workflowdoc.root.attributes?
        tt = @workflowdoc.root.attributes["taskthrottle"]
        if tt.nil?
          nil
        else
          tt.to_i
        end
      end
    end

    ##########################################
    #
    # metatask_throttles
    #
    ##########################################
    attr_reader :metatask_throttles

    ##########################################
    #
    # corethrottle
    #
    ##########################################
    def corethrottle
      if @workflowdoc.root.attributes?
        ct = @workflowdoc.root["corethrottle"]
        if ct.nil?
          nil
        else
          ct.to_i
        end
      end
    end

    ##########################################
    #
    # scheduler
    #
    ##########################################
    def scheduler
      if @workflowdoc.root.attributes?
        sched = @workflowdoc.root["scheduler"]
        if sched.nil?
          nil
        else
          WorkflowMgr.const_get("#{sched.upcase}BatchSystem").new(@config)
        end
      end
    end

    ##########################################
    #
    # features_supported?
    #
    ##########################################
    def features_supported?
      supported = false
      if @workflowdoc.root.attributes?
        sched = @workflowdoc.root["scheduler"]
        unless sched.nil?
          clazz = WorkflowMgr.const_get("#{sched.upcase}BatchSystem")
          supported = true
          @features_used.each_key do |feature|
            next if clazz.feature?(feature)

            supported = false
            WorkflowMgr.stderr(@@messages[feature] || "Feature '#{feature}' is unsupported on this platform.")
          end
        end
      end
      supported
    end

    ##########################################
    #
    # log
    #
    ##########################################
    def log
      lognodes = @workflowdoc.find('/workflow/log')
      lognode = lognodes.first
      path = get_compound_time_string(lognode)
      verbosity = lognode.attributes['verbosity']
      verbosity = verbosity.to_i unless verbosity.nil?

      WorkflowLog.new(path, verbosity, @workflow_io_server)
    end

    ##########################################
    #
    # cycledefs
    #
    ##########################################
    def cycledefs
      cycles = []
      cyclenodes = @workflowdoc.find('/workflow/cycledef')
      cyclenodes.each do |cyclenode|
        cyclenode.output_escaping = false
        cyclefields = unescape(cyclenode.content.strip)
        nfields = cyclefields.split.size
        group = cyclenode.attributes['group']
        activation_offset = if realtime?
                              WorkflowMgr.ddhhmmss_to_seconds(cyclenode.attributes['activation_offset'])
                            else
                              0
                            end
        exclude_hours = cyclenode.attributes['exclude_hours']
        valid_hours = cyclenode.attributes['valid_hours']
        if nfields == 3
          cycles << CycleInterval.new(cyclefields, group, activation_offset, nil, exclude_hours, valid_hours)
        elsif nfields == 6
          if exclude_hours || valid_hours
            raise "ERROR: 'exclude_hours' or 'valid_hours' not supported in cron-style <cycledef>."
          end

          cycles << CycleCron.new(cyclefields, group, activation_offset)
        else
          raise "ERROR: Unsupported <cycle> type!"
        end
      end

      cycles
    end

    ##########################################
    #
    # tasks
    #
    ##########################################
    def tasks
      tasks = {}
      tasknodes = @workflowdoc.find('/workflow/task')
      tasknodes.each_with_index do |tasknode, seq|
        natives = []
        rewinders = []
        taskattrs = {}
        taskenvars = {}
        taskdep = nil
        taskhangdep = nil
        # Get task attributes insde the <task> tag
        tasknode.attributes.each do |attr|
          attrkey = attr.name.to_sym
          attrval = case attrkey
                    when :maxtries, :throttle # Attributes with integer values go here
                      attr.value.to_i
                    when :final # Attributes with boolean values go here
                      !(attr.value.downcase =~ /^t|true$/).nil?
                    else # Attributes with string values
                      attr.value
                    end
          taskattrs[attrkey] = attrval
        end

        # Get task attributes, envars, and dependencies declared as elements inside <task> element
        tasknode.each_element do |e|
          case e.name
          when /shared/
            taskattrs[:shared] = true
            @features_used[:shared] = true
          when /exclusive/
            taskattrs[:exclusive] = true
            @features_used[:exclusive] = true
          when /^envar$/
            envar_name = nil
            envar_value = nil
            e.each_element do |element|
              case element.name
              when /^name$/
                envar_name = get_compound_time_string(element)
              when /^value$/
                envar_value = get_compound_time_string(element)
              end
            end
            taskenvars[envar_name] = envar_value
          when /^rewind$/
            e.each_element do |element|
              if element.name == 'rb'
                rewinders.push(get_rubydep(element))
              elsif element.name == 'sh'
                rewinders.push(get_shelldep(element))
              else
                raise "Invalid tag <#{element.name}> inside <rewind> tag: #{element.node_type_name}"
              end
            end
          when /^dependency$/
            e.each_element do |element|
              raise "ERROR: <dependency> tag contains too many elements" unless taskdep.nil?

              taskdep = Dependency.new(get_dependency_node(element))
            end
          when /^hangdependency$/
            e.each_element do |element|
              raise "ERROR: <hangdependency> tag contains too many elements" unless taskhangdep.nil?

              taskhangdep = Dependency.new(get_dependency_node(element))
            end
          else
            attrkey = e.name.to_sym
            case attrkey
            when :cores # <task> elements with integer values go here
              e.output_escaping = false
              attrval = unescape(e.content).to_i
            when :nodes
              e.output_escaping = false
              attrval = unescape(e.content)
              taskattrs[:cores] = 0
              attrval.split("+").each do |nodespec|
                resources = nodespec.split(":")
                nodes = resources.shift.to_i
                tpp = 1
                ppn = 0
                resources.each do |resource|
                  case resource
                  when /ppn=(\d+)/
                    ppn = ::Regexp.last_match(1).to_i
                  when /tpp=(\d+)/
                    tpp = ::Regexp.last_match(1).to_i
                  end
                end
                raise "ERROR: <node> tag must contain a :ppn setting for each nodespec" if ppn == 0

                taskattrs[:cores] += nodes * ppn * tpp
              end
            when :nodesize
              e.output_escaping = false
              attrval = unescape(e.content).to_i
            else # <task> elements with compoundtimestring values
              attrval = get_compound_time_string(e)
            end
            if attrkey.to_s == 'native'
              natives.push(attrval)
            else
              taskattrs[attrkey] = attrval
            end
          end
        end

        task = Task.new(seq, taskattrs, taskenvars, taskdep, taskhangdep)
        tasks[task.attributes[:name]] = task
        rewinders.each do |rewinder|
          task.add_rewind_action(rewinder)
        end
        inv = 0
        natives.each do |native|
          task.add_native(native)
          inv += 1
        end

        knnv = 0
        task.each_native do |native|
          knnv += 1
        end
        if knnv != inv
          raise "ERROR: Ruby did not add all objects to the internal list: #{knnv}!=#{inv}"
        end
      end

      tasks
    end

    ##########################################
    #
    # taskdep_cycle_offsets
    #
    ##########################################
    def taskdep_cycle_offsets
      offsets = []
      taskdepnodes = @workflowdoc.find('//taskdep')
      taskdepnodes.each do |taskdepnode|
        offsets << WorkflowMgr.ddhhmmss_to_seconds(taskdepnode.attributes["cycle_offset"]) \
          unless taskdepnode.attributes["cycle_offset"].nil?
      end

      taskdepnodes = @workflowdoc.find('//cycleexistdep')
      taskdepnodes.each do |taskdepnode|
        offsets << WorkflowMgr.ddhhmmss_to_seconds(taskdepnode.attributes["cycle_offset"]) \
          unless taskdepnode.attributes["cycle_offset"].nil?
      end

      offsets.uniq
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

      strarray = []
      element.each do |e|
        e.output_escaping = false
        if e.node_type == LibXML::XML::Node::TEXT_NODE
          strarray << unescape(e.content)
        elsif e.node_type == LibXML::XML::Node::COMMENT_NODE
          # Ignore comments
        else
          offset_sec = WorkflowMgr.ddhhmmss_to_seconds(e.attributes["offset"])
          case e.name
          when "cyclestr"
            formatstr = unescape(e.content)
            formatstr.gsub!(/%/, '%%')
            formatstr.gsub!(/@(\^?[^@\s])/, '%\1')
            formatstr.gsub!(/@@/, '@')
            strarray << CycleString.new(formatstr, offset_sec)
          else
            raise "Invalid tag <#{e.name}> inside #{element}: #{e.node_type_name}"
          end
        end
      end

      CompoundTimeString.new(strarray)
    end

    ##########################################
    #
    # get_dependency_node
    #
    ##########################################
    def get_dependency_node(element)
      # Build a dependency tree
      children = []
      element.each_element { |e| children << e }
      case element.name
      when "not"
        DependencyNotOperator.new(children.collect { |child| get_dependency_node(child) })
      when "and"
        DependencyAndOperator.new(children.collect { |child| get_dependency_node(child) })
      when "or"
        DependencyOrOperator.new(children.collect { |child|  get_dependency_node(child) })
      when "nand"
        DependencyNandOperator.new(children.collect { |child| get_dependency_node(child) })
      when "nor"
        DependencyNorOperator.new(children.collect { |child|  get_dependency_node(child) })
      when "xor"
        DependencyXorOperator.new(children.collect { |child|  get_dependency_node(child) })
      when "some"
        DependencySomeOperator.new(children.collect do |child|
          get_dependency_node(child)
        end, element.attributes["threshold"].to_f)
      when "taskdep"
        get_taskdep(element)
      when "streq", "strneq", "true", "false"
        get_constdep(element)
      when "rb"
        get_rubydep(element)
      when "sh"
        get_shelldep(element)
      when "cycleexistdep"
        get_cycleexistdep(element)
      when "taskvalid"
        get_taskvaliddep(element)
      when "datadep"
        get_datadep(element)
      when "timedep"
        get_timedep(element)
      end
    end

    #####################################################
    #
    # get_constdep
    #
    #####################################################
    def get_constdep(element)
      case element.name
      when 'true'
        ConstDependency.new(true, 'true')
      when 'false'
        ConstDependency.new(false, 'false')
      when 'streq', 'strneq'
        left = get_compound_time_string(element.find('left').first)
        right = get_compound_time_string(element.find('right').first)
        if element.name == 'streq'
          name = name_stringdep(left, right, '==')
          compare = '=='
        else
          name = name_stringdep(left, right, "!=")
          compare = '!='
        end
        StringDependency.new(left, right, name, compare)
      else
        raise "Invalid constant dependency #{element.name}"
      end
    end

    #####################################################
    #
    # name_stringdep
    #
    #####################################################

    def name_stringdep(left, right, cmp)
      ia = left.inspect
      ib = right.inspect
      cmp = cmp.to_s
      ia = "#{ia[0..37]}..." if ia.size > 40
      ib = "#{ib[0..37]}..." if ib.size > 40
      cmp = "#{cmp[0..37]}..." if cmp.size > 40
      "'#{ia}'#{cmp}'#{ib}'"
    end

    #####################################################
    #
    # get_taskdep
    #
    #####################################################
    def get_taskdep(element)
      # Get the mandatory task attribute
      task = element.attributes["task"]

      # Get the state attribute
      state = element.attributes["state"] || "SUCCEEDED"

      # Get the cycle offset, if there is one
      cycle_offset = WorkflowMgr.ddhhmmss_to_seconds(element.attributes["cycle_offset"]) || 0

      TaskDependency.new(task, state, cycle_offset)
    end

    #####################################################
    #
    # get_cycleexistdep
    #
    #####################################################
    def get_cycleexistdep(element)
      # Get the cycle offset, if there is one
      cycle_offset = WorkflowMgr.ddhhmmss_to_seconds(element.attributes["cycle_offset"])

      CycleExistDependency.new(cycle_offset)
    end

    #####################################################
    #
    # get_taskvaliddep
    #
    #####################################################
    def get_taskvaliddep(element)
      task = element.attributes["task"]

      TaskValidDependency.new(task)
    end

    #####################################################
    #
    # get_rubydep
    #
    #####################################################
    def get_rubydep(element)
      # Get the cycle offset, if there is one
      name = element.attributes["name"]
      text = get_compound_time_string(element)
      RubyDependency.new(text, name)
    end

    #####################################################
    #
    # get_shelldep
    #
    #####################################################
    def get_shelldep(element)
      # Get the cycle offset, if there is one
      name = element.attributes["name"]
      shell = element.attributes["shell"]
      runopt = element.attributes["runopt"]
      if shell.nil?
        shell = '/bin/sh' # POSIX requires this location for the POSIX sh
      end
      if runopt.nil?
        runopt = '-c' # sh -c (command)   -- need the -c
      end
      text = get_compound_time_string(element)
      ShellDependency.new(shell, runopt, text, name)
    end

    ##########################################
    #
    # get_datadep
    #
    ##########################################
    def get_datadep(element)
      # Get the age attribute
      age_sec = WorkflowMgr.ddhhmmss_to_seconds(element.attributes["age"]) || 0

      # Get the minsize attribute
      minsize = element.attributes["minsize"] || 0
      case minsize
      when /^(\d+)$/
        minsize = ::Regexp.last_match(1).to_i
      when /^(\d+)[B|b]$/
        minsize = ::Regexp.last_match(1).to_i
      when /^(\d+)[K|k]$/
        minsize = ::Regexp.last_match(1).to_i * 1024
      when /^(\d+)[M|m]$/
        minsize = ::Regexp.last_match(1).to_i * 1024 * 1024
      when /^(\d+)[G|g]$/
        minsize = ::Regexp.last_match(1).to_i * 1024 * 1024 * 1024
      when /^(\d+)[T|t]$/
        minsize = ::Regexp.last_match(1).to_i * 1024 * 1024 * 1024 * 1024
      when /^(\d+)[P|p]$/
        minsize = ::Regexp.last_match(1).to_i * 1024 * 1024 * 1024 * 1024 * 1024
      when /^(\d+)[E|e]$/
        minsize = ::Regexp.last_match(1).to_i * 1024 * 1024 * 1024 * 1024 * 1024 * 1024
      end

      DataDependency.new(get_compound_time_string(element), age_sec, minsize)
    end

    #####################################################
    #
    # get_timedep
    #
    #####################################################
    def get_timedep(element)
      # Get the time cycle string
      TimeDependency.new(get_compound_time_string(element))
    end

    ##########################################
    #
    # validate_with_metatasks
    #
    ##########################################
    def validate_with_metatasks(doc)
      # Parse the Relax NG schema XML document
      xmlstring = @workflow_io_server.parse_xml_file("#{File.dirname(__FILE__)}/schema_with_metatasks.rng")
      relaxng_document = LibXML::XML::Parser.string(xmlstring, options: LibXML::XML::Parser::Options::NOENT).parse

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
      xmlstring = @workflow_io_server.parse_xml_file("#{File.dirname(__FILE__)}/schema_without_metatasks.rng")
      relaxng_document = LibXML::XML::Parser.string(xmlstring, options: LibXML::XML::Parser::Options::NOENT).parse

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
      @workflowdoc.root.each_element do |ch|
        next unless ch.name == "task"

        # Find the metataskdep nodes
        metataskdeps = ch.find('.//metataskdep')

        # Replace each of them with the equivalient <and><taskdep/><taskdep/>...</and> expression
        metataskelements = []
        metataskdeps.each do |metataskdep|
          metataskelements << metataskdep

          # Get the name of the metatask
          metatask = metataskdep.attributes["metatask"]

          # Find all names of tasks descended from the metatask
          tasknames = []
          tasks = @workflowdoc.find("//task[contains(@metatasks,'#{metatask}')]")
          tasks.each do |task|
            tasknames << task.attributes["name"] if task.attributes["metatasks"] =~ /^([^,]+,)*#{metatask}(,[^,]+)*$/
          end

          # Insert a "some" element after the metataskdep element
          somenode = LibXML::XML::Node.new("some")
          threshold = metataskdep.attributes["threshold"].nil? ? "1.0" : metataskdep.attributes["threshold"]
          LibXML::XML::Attr.new(somenode, "threshold", threshold)
          metataskdep.next = somenode

          # Add taskdep elements as children to the and element
          tasknames.each do |task|
            taskdepnode = LibXML::XML::Node.new("taskdep")
            LibXML::XML::Attr.new(taskdepnode, "task", task)
            unless metataskdep["cycle_offset"].nil?
              LibXML::XML::Attr.new(taskdepnode, "cycle_offset",
                                    metataskdep.attributes["cycle_offset"])
            end
            unless metataskdep.attributes["state"].nil?
              LibXML::XML::Attr.new(taskdepnode, "state",
                                    metataskdep.attributes["state"])
            end
            somenode << taskdepnode
          end
        end

        # Remove the metataskdep elements
        metataskelements.each(&:remove!)
      end
    end

    ##########################################
    #
    # expand_serialdeps
    #
    ##########################################
    def expand_serialdeps
      # workflowdoc.root.each
      @workflowdoc.root.each_element do |ch|
        next unless ch.name == "task"

        # Skip tasks that are not members of a metatask
        next if ch.attributes["metatasks"].nil?

        depnode = nil
        andnode = nil

        # Add dependencies for each serial metatask that this task is a member of
        metatasklist = ch.attributes["metatasks"]
        metatasklist.split(",").each_with_index do |m, idx|
          # Ignore parallel metatasks
          next unless @metatask_modes[m] == "serial"

          # Find the seqnum for the tasks on which this task depends
          seqdeplist = ch.attributes["seqnum"]
          seqdeps = seqdeplist.split(",")[0..idx].collect(&:to_i)
          seqdeps[idx] -= 1

          # Unless this is the first task in the sequence, it has a dependency for this metatask
          next unless seqdeps[idx] > 0

          # Find the <dependency> node for this task, or make one if it isn't found
          if depnode.nil?
            depnode = ch.find_first("./dependency")
            if depnode.nil?
              depnode = LibXML::XML::Node.new("dependency")
              andnode = LibXML::XML::Node.new("and")
              ch << depnode
              depnode << andnode
            else
              depchild = depnode.find_first("./*[1]")
              if depchild.name == "and"
                andnode = depchild
              else
                depchildren = depnode.children
                andnode = LibXML::XML::Node.new("and")
                depnode << andnode
                depchildren.each do |c|
                  andnode << c
                end
              end
            end
          end

          # Find all tasks that match the sequence number for dependent tasks
          tasks1 = @workflowdoc.find("//task[starts-with(@seqnum,'#{seqdeps.join(',')},')]")
          tasks2 = @workflowdoc.find("//task[@seqnum='#{seqdeps.join(',')}']")
          tasks = tasks1.to_a | tasks2.to_a

          # Insert a task dep for each dependent task
          tasks.each do |t|
            # Reject tasks that aren't a member of metatask m
            next if t.attributes["metatasks"].split(",").find_index(m).nil?

            taskdepnode = LibXML::XML::Node.new("taskdep")
            LibXML::XML::Attr.new(taskdepnode, "task", t.attributes["name"])
            andnode << taskdepnode
          end

          # if seqdeps[idx]

          # if @metatask_modes
        end

        # if ch.name
      end
    end

    ##########################################
    #
    # expand_metatasks
    #
    ##########################################
    def expand_metatasks
      # Parse and expand metatasks
      metatasks = []
      @metatask_seq = 1
      @metatask_throttles = {}
      @metatask_modes = {}
      @workflowdoc.root.each_element do |ch|
        next unless ch.name == "metatask"

        if ch.attributes["name"].nil?
          LibXML::XML::Attr.new(ch, "name", "metatask#{@metatask_seq}")
          @metatask_seq += 1
        end
        metatask_name = ch.attributes["name"]
        @metatask_throttles[metatask_name] = ch.attributes["throttle"].nil? ? 999_999 : ch.attributes["throttle"].to_i
        @metatask_modes[metatask_name] = ch.attributes["mode"].nil? ? "parallel" : ch.attributes["mode"]
        pre_parse(ch, metatask_name)
        metatasks << ch
      end
      metatasks.each(&:remove!)
    end

    #####################################################
    #
    # traverse
    #
    #####################################################
    def traverse(node, id_table, index)
      if node.node_type_name == "text"
        node.output_escaping = false
        cont = unescape(node.content)
        id_table.each_key do |id|
          next while cont.sub!("##{id}#", id_table[id][index])
        end
        node.content = cont

      else
        node.attributes.each do |attr|
          val = attr.value
          id_table.each_key do |id|
            next while val.sub!("##{id}#", id_table[id][index])
          end
          attr.value = val
        end
        node.children.each { |ch| traverse(ch, id_table, index) }
      end
    end

    #####################################################
    #
    # pre-parse
    #
    #####################################################
    def pre_parse(metatask, metatask_list)
      id_table = {}
      var_length = -1

      # Set the metatask list for all task children of this metatask
      seqnum = 0
      if metatask.attributes["seqnum"].nil?
        LibXML::XML::Attr.new(metatask, "seqnum", "")
      else
        metatask.attributes["seqnum"] += ","
      end
      metatask.children.each do |e|
        if e.name == "task"
          e.attributes["metatasks"] = metatask_list
          e.attributes["seqnum"] = metatask["seqnum"]
          seqnum += 1
          e.attributes["seqnum"] += seqnum.to_s
        elsif e.name == "metatask"
          e.attributes["seqnum"] = metatask.attributes["seqnum"]
          seqnum += 1
          e.attributes["seqnum"] += seqnum.to_s
        end
      end

      # Parse each metatask child of this metatask, adding the metatask child to the metatask list
      metatask.children.each do |ch|
        next unless ch.name == "metatask"

        if ch.attributes["name"].nil?
          LibXML::XML::Attr.new(ch, "name", "metatask#{@metatask_seq}")
          @metatask_seq += 1
        end
        metatask_name = ch.attributes["name"]
        @metatask_throttles[metatask_name] = ch.attributes["throttle"].nil? ? 999_999 : ch.attributes["throttle"].to_i
        @metatask_modes[metatask_name] = ch.attributes["mode"].nil? ? "parallel" : ch.attributes["mode"]
        pre_parse(ch, metatask_list + ",#{metatask_name}")
      end

      # Build a table of var tags and their values for this metatask
      metatask.children.each do |e|
        next unless e.name == "var"

        e.output_escaping = false
        var_values = unescape(e.content).split
        var_length = var_values.length if var_length == -1
        raise "ERROR: <var> tags do not contain the same number of items!" if var_values.length != var_length

        id_table[e.attributes["name"]] = var_values
      end
      raise "ERROR: No <var> tag or values specified in one or more metatasks" if var_length < 1

      # Expand the metatasks, adding metatask list only to the expanded tasks from nested metatasks
      task_list = []
      depth = metatask_list.split(",").size
      maxseq = 0
      0.upto(var_length - 1) do |index|
        metatask.children.each do |e|
          next unless e.name == "task"

          task_copy = e.copy(true)
          if task_copy.attributes["metatasks"].nil?
            LibXML::XML::Attr.new(task_copy, "metatasks", metatask_list)
          end
          traverse(task_copy, id_table, index)
          seqarr = task_copy.attributes["seqnum"].split(",")
          if index == 0
            maxseq = seqarr[depth - 1].to_i
          else
            seqarr[depth - 1] = seqarr[depth - 1].to_i + maxseq * index
            task_copy.attributes["seqnum"] = seqarr.join(",")
          end
          task_list << task_copy
        end
      end

      # Insert the expanded tasks into the XML tree
      (task_list.length - 1).downto(0) { |x| metatask.next = task_list[x] }
    end
  end
end
