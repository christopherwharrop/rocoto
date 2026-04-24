#! /bin/env ruby

##########################################
#
# Module WorkflowMgr
#
##########################################
module WorkflowMgr
  require 'workflowmgr/compoundtimestring'

  ##########################################
  #
  # StringEvaluator
  #
  ##########################################
  class StringEvaluator
    ##########################################
    #
    # initialize
    #
    ##########################################
    def initialize
      @vars = {}
      @defaults = {}
    end

    ##########################################
    #
    # Lexical Scope Query and Editing
    #
    # []= [] var? each_var
    #
    ##########################################
    def []=(var, value)
      raise 'var must be a string' unless var.is_a?(String)
      raise 'value must be a string' unless value.is_a?(String)

      svar = var.to_s
      var_name_ok?(var)
      @vars[svar] = value
    end

    def [](var)
      svar = var.to_s
      if @vars.key?(svar)
        @vars[svar]
      elsif @defaults.key?(svar)
        @defaults[svar]
      end
    end

    def var?(var)
      svar = var.to_s
      @vars.key?(svar) || @defaults.key?(svar)
    end

    def each_var(&block)
      @vars.each(&block)
      @defaults.each do |k, v|
        unless @vars.key?(k)
          yield k, v
        end
      end
    end

    ##########################################
    #
    # Add "default" values to scope that []=
    # will override
    #
    # defaults_from, setdef, getdef
    #
    ##########################################
    def defaults_from(strev)
      # Sets the other StringEvaluator's variables and defaults to be
      # this StringEvaluator's defaults, unless other defaults or
      # variables are already set.
      strev.each_var do |k, v|
        next if @defaults.key?(k)
        raise 'key must be a string in defaults_from' unless k.is_a?(String)
        raise 'value must be a string in defaults_from' unless v.is_a?(String)

        setdef(k, v)
      end
    end

    def setdef(var, value)
      raise 'var must be a string in setdef' unless var.is_a?(String)

      svar = var.to_s
      var_name_ok?(var)
      @defaults[svar] = value
      value
    end

    def getdef(var)
      svar = var.to_s
      defaults[svar]
    end

    ##########################################
    #
    # set_* -- Add Groups of Variables
    #
    ##########################################
    # Add a Task and its name
    def add_task(name, task = nil)
      @defaults['taskname'] = name.to_s # taskname = task name
      unless task.nil?
        @defaults['taskobj'] = task # taskobj = the Task
        @defaults['env'] = task.envars # env = the Task's env vars
        @defaults['seq'] = task.seq # seq = Task's index location
      end
      self
    end

    # Add a WorkflowXMLDoc
    def add_doc(doc)
      @defaults['doc'] = doc # doc = the WorkflowXMLDoc
    end

    # Add a Cycle
    def add_cycle(cycle)
      @defaults['cycle'] = cycle # cycle = the cycle time

      @defaults['evalcycle'] = cycle # evalcycle = the Cycle object again
      # (User cannot set "evalcycle" in XML.)

      # Various common aliases for portions of the time:
      @defaults['ymd'] = cycle.strftime('%Y%m%d')
      @defaults['ymdh'] = cycle.strftime('%Y%m%d%H')
      @defaults['ymdhm'] = cycle.strftime('%Y%m%d%H%M')
      @defaults['ymdhms'] = cycle.strftime('%Y%m%d%H%M%S')
      @defaults['hms'] = cycle.strftime('%H%M%S')
      @defaults['century'] = cycle.strftime('%C')
      @defaults['year'] = cycle.strftime('%Y')
      @defaults['month'] = cycle.strftime('%m')
      @defaults['day'] = cycle.strftime('%d')
      @defaults['hour'] = cycle.strftime('%H')
      @defaults['minute'] = cycle.strftime('%M')
      @defaults['second'] = cycle.strftime('%S')
      @defaults['doy'] = cycle.strftime('%j')
      @defaults['cycleepoch'] = cycle.to_i
    end

    ##########################################
    #
    # Execution
    #
    ##########################################
    # execute, return a boolean
    def run_bool(evalstr)
      return true if run(evalstr)

      false
    end

    # execute, return a string
    def run_str(evalstr)
      run(evalstr).to_s
    end

    # execute, return result of eval
    #
    # DEPRECATED: The <rb> tag feature is deprecated and will be removed in a future release.
    # Evaluating arbitrary Ruby expressions from workflow XML cannot be made safe without
    # AST-level sandboxing, which is not implemented. Use <sh> tags or workflow
    # dependencies instead.
    def run(evalstr)
      evalstr = evalstr.to_s

      warn '[DEPRECATION] The <rb> tag evaluates arbitrary Ruby code and will be removed ' \
           'in a future version of Rocoto. Please replace <rb> expressions with <sh> tags ' \
           'or workflow dependencies.'

      # Get a binding within the make_binding() subroutine of a copy of
      # this object.  Using a clone shields us from permanent
      # modifications of @vars or @defaults.
      evalbind = clone.make_binding

      # Construct a new command to evaluate, which first defines the
      # requested variables and functions.
      evalcmd = ''
      evalline = 1
      each_var do |k, v|
        evalcmd += "#{k} = self['#{k}']\n"
        evalline -= 1
        next unless k == 'evalcycle'
        # If a cycle is present, define a new cyclestr function that
        # acts like the <cyclestr> tag.
        # evalcmd+="def cyclestr(s) ; evalcycle.cycle.strftime(s.gsub('%','@')) ; end"
        # evalline -= 1
        # (DOES NOT WORK -- KEEP COMMENTED FOR NOW)
      end

      # Append the requested string to that comand:
      evalcmd += evalstr

      # The string to print when listing error messages:
      errstr = if evalstr.size > 20
                 "<rb>#{evalstr[0..17]}...</rb>"
               else
                 "<rb>#{evalstr}</rb>"
               end

      # Evaluate the command in the Binding we made earlier.
      # NOTE: Security/Eval does not flag Binding#eval (only Kernel#eval).
      # A blocklist cannot safely restrict arbitrary Ruby; this feature is
      # deprecated pending removal.
      evalbind.eval(evalcmd, errstr, evalline)
    end

    ##########################################
    #
    # shell_bool
    #
    ##########################################
    def shell_bool(shell, runopt, evalexpr, cycle)
      # shell     -->  "/bin/sh"
      # runopt    -->  "-c"
      # evalexpr  -->  "echo hello world"
      save_env = {}
      ENV.each do |k, v|
        save_env[k] = v
      end
      begin
        each_var do |k, v|
          case v
          when CompoundTimeString
            ENV[k.to_s] = v.to_s(cycle)
          when Hash
            if k == 'env'
              v.each do |k2, v2|
                if v2.is_a?(CompoundTimeString)
                  v2s = v2.to_s(cycle)
                  ENV[k2.to_s] = v2s
                elsif v.is_a?(String)
                  ENV[k2.to_s] = v2
                end
              end
            end
          when Array
            # Skip arrays
          when Task
            # Skip task objects
          when WorkflowIOProxy
            # Skip IO proxy objects
          else
            ENV[k.to_s] = v.to_s
          end
        end
        result = system(shell, runopt, evalexpr)
        if result
          true
        else
          false
        end
      ensure
        ENV.clear
        save_env.each do |k, v|
          ENV[k] = v
        end
      end
    end

    ##########################################
    #
    # make_binding
    # Binding generation
    #
    ##########################################
    def make_binding
      # This function lets us do clone.make_binding to make a binding
      # in a clone of ourself.  That is used to prevent user scripts
      # from accidentally making permanent changes to @vars or
      # @defaults.
      binding
    end

    @@reserved_words = Set.new \
      ['BEGIN', 'END', 'alias', 'and', 'begin', 'break', 'case',
       'class', 'def', 'defined', 'do', 'else', 'elsif', 'end',
       'ensure', 'false', 'for', 'if', 'module', 'next', 'nil', 'not',
       'or', 'redo', 'rescue', 'retry', 'return', 'self', 'super',
       'then', 'true', 'undef', 'unless', 'until', 'when', 'while',
       'yield', '__LINE__', '__FILE__']

    @@reserved_vars = Set.new ['evalstr', 'evalbind', 'evalcycle']

    ##########################################
    #
    # var_name_ok?
    # Checking variable names.  Raises an
    # exception if a name is invalid.
    #
    ##########################################
    def var_name_ok?(varname)
      svar = varname.to_s
      if @@reserved_words.include?(svar)
        raise ArgumentError, "Name \"#{svar}\" is a reserved word in Ruby."
      elsif @@reserved_vars.include?(svar)
        raise ArgumentError, "Cannot override the #{svar} variable."
      elsif /\A[a-zA-Z][a-zA-Z0-9_]*\z/.match(svar)
        svar
      else
        raise ArgumentError,
              "Invalid variable name \"#{var}\": it must be a letter followed by " \
              "any number of letters, numbers and underscores."
      end
    end
  end
end
