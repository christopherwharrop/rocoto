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
    require 'set'

    ##########################################
    #
    # initialize
    #
    ##########################################
    def initialize()
      @vars=Hash.new
      @defaults=Hash.new
    end

    ##########################################
    #
    # Lexical Scope Query and Editing
    #
    # []= [] has_var? each_var
    #
    ##########################################
    def []=(var,value)
      raise 'var must be a string' unless var.is_a?(String)
      raise 'value must be a string' unless value.is_a?(String)
      svar=var.to_s
      var_name_ok?(var)
      @vars[svar]=value
      return value
    end

    def [](var)
      svar=var.to_s
      if @vars.has_key?(svar)
        return @vars[svar]
      elsif @defaults.has_key?(svar)
        return @defaults[svar]
      else
        return nil
      end
    end

    def has_var?(var)
      svar=var.to_s
      return @vars.has_key?(svar) || @defaults.has_key?(svar)
    end

    def each_var()
      @vars.each do |k,v|
        yield k,v
      end
      @defaults.each do |k,v|
        if ! @vars.has_key?(k)
          yield k,v
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
      strev.each_var do |k,v|
        if not @defaults.has_key?(k)
          raise 'key must be a string in defaults_from' unless k.is_a?(String)
          raise 'value must be a string in defaults_from' unless v.is_a?(String)
          setdef(k,v)
        end
      end
    end

    def setdef(var,value)
      raise 'var must be a string in setdef' unless var.is_a?(String)
      svar=var.to_s
      var_name_ok?(var)
      @defaults[svar]=value
      return value
    end

    def getdef(var)
      svar=var.to_s
      return defaults[svar]
    end

    ##########################################
    #
    # set_* -- Add Groups of Variables
    #
    ##########################################
    def set_task(name,task=nil)  # Add a Task and its name
      @defaults['taskname']=name.to_s   # taskname = task name
      if ! task.nil?
        @defaults['taskobj']=task   # taskobj = the Task
        @defaults['env']=task.envars # env = the Task's env vars
        @defaults['seq']=task.seq   # seq = Task's index location
      end
      return self
    end

    def set_doc(doc) # Add a WorkflowXMLDoc
      @defaults['doc']=doc          # doc = the WorkflowXMLDoc
    end

    def set_cycle(cycle) # Add a Cycle
      @defaults['cycle']=cycle # cycle = the cycle time

      @defaults['evalcycle']=cycle # evalcycle = the Cycle object again
      # (User cannot set "evalcycle" in XML.)

      # Various common aliases for portions of the time:
      @defaults['ymd']=cycle.strftime('%Y%m%d')
      @defaults['ymdh']=cycle.strftime('%Y%m%d%H')
      @defaults['ymdhm']=cycle.strftime('%Y%m%d%H%M')
      @defaults['ymdhms']=cycle.strftime('%Y%m%d%H%M%S')
      @defaults['hms']=cycle.strftime('%H%M%S')
      @defaults['century']=cycle.strftime('%C')
      @defaults['year']=cycle.strftime('%Y')
      @defaults['month']=cycle.strftime('%m')
      @defaults['day']=cycle.strftime('%d')
      @defaults['hour']=cycle.strftime('%H')
      @defaults['minute']=cycle.strftime('%M')
      @defaults['second']=cycle.strftime('%S')
      @defaults['doy']=cycle.strftime('%j')
      @defaults['cycleepoch']=cycle.to_i
    end

    ##########################################
    #
    # Execution
    #
    ##########################################
    def run_bool(evalstr) # execute, return a boolean
      return true if run(evalstr)
      return false
    end

    def run_str(evalstr) # execute, return a string
      return run(evalstr).to_s
    end

    def run(evalstr) # execute, return result of eval
      evalstr=evalstr.to_s

      # Get a binding within the get_binding() subroutine of a copy of
      # this object.  Using a clone shields us from permanent
      # modifications of @vars or @defaults.
      evalbind=clone.get_binding

      # Construct a new command to evaluate, which first defines the
      # requested variables and functions.
      evalcmd=''
      evalline=1
      each_var do |k,v|
        evalcmd += "#{k.to_s} = self['#{k.to_s}']\n"
        evalline -= 1
        if k=='evalcycle'
          # If a cycle is present, define a new cyclestr function that
          # acts like the <cyclestr> tag.
          #evalcmd+="def cyclestr(s) ; evalcycle.cycle.strftime(s.gsub('%','@')) ; end"
          #evalline -= 1
          # (DOES NOT WORK -- KEEP COMMENTED FOR NOW)
        end
      end

      # Append the requested string to that comand:
      evalcmd+=evalstr

      # The string to print when listing error messages:
      if evalstr.size>20
        errstr="<rb>#{evalstr[0..17]}...</rb>"
      else
        errstr="<rb>#{evalstr}</rb>"
      end

      # Evaluate the command in the Binding we made earlier:
      result= evalbind.eval(evalcmd,errstr,evalline)
      return result
    end

    ##########################################
    #
    # shell_bool
    #
    ##########################################
    def shell_bool(shell,runopt,evalexpr,cycle)
      # shell     -->  "/bin/sh"
      # runopt    -->  "-c"
      # evalexpr  -->  "echo hello world"
      save_env=Hash.new
      ENV.each do |k,v|
        save_env[k]=v
      end
      begin
       each_var do |k,v|
          if v.is_a?(CompoundTimeString)
            ENV[k.to_s]=v.to_s(cycle)
          elsif v.is_a?(Hash)
            if k=='env'
              v.each do |k2,v2|
                if v2.is_a?(CompoundTimeString)
                  v2s=v2.to_s(cycle)
                  ENV[k2.to_s] = v2s
                elsif v.is_a?(String)
                  ENV[k2.to_s] = v2
                end
              end
            else
              # Skip other hashes.
            end
          else
            ENV[k.to_s]=v.to_s
          end
        end
        result=system(shell,runopt,evalexpr)
        if(result)
          return true
        else
          return false
        end
      ensure
        ENV.clear
        save_env.each do |k,v|
          ENV[k]=v
        end
      end
    end

    ##########################################
    #
    # get_binding
    # Binding generation
    #
    ##########################################
    def get_binding()
      # This function lets us do clone.get_binding to make a binding
      # in a clone of ourself.  That is used to prevent user scripts
      # from accidentally making permanent changes to @vars or
      # @defaults.
      return binding()
    end

    @@reserved_words=Set.new \
    [ 'BEGIN', 'END', 'alias', 'and', 'begin', 'break', 'case',
      'class', 'def', 'defined', 'do', 'else', 'elsif', 'end',
      'ensure', 'false', 'for', 'if', 'module', 'next', 'nil', 'not',
      'or', 'redo', 'rescue', 'retry', 'return', 'self', 'super',
      'then', 'true', 'undef', 'unless', 'until', 'when', 'while',
      'yield', '__LINE__', '__FILE__' ]

    @@reserved_vars=Set.new [ 'evalstr', 'evalbind', 'evalcycle' ]

    ##########################################
    #
    # var_name_ok?
    # Checking variable names.  Raises an
    # exception if a name is invalid.
    #
    ##########################################
    def var_name_ok?(varname)
      svar=varname.to_s
      if @@reserved_words.include?(svar)
        raise ArgumentError, "Name \"#{svar}\" is a reserved word in Ruby."
      elsif @@reserved_vars.include?(svar)
        raise ArgumentError, "Cannot override the #{svar} variable."
      elsif /\A[a-zA-Z][a-zA-Z0-9_]*\z/.match(svar)
        return svar
      else
        raise ArgumentError, "Invalid variable name \"#{var}\": it must be a letter followed by any number of letters, numbers and underscores."
      end
    end
  end
end
