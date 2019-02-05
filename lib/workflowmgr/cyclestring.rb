##########################################
#
# Module WorkflowMgr
#
##########################################
module WorkflowMgr

  require 'date'

  ##########################################
  #
  # Class CycleString
  #
  ##########################################
  class CycleString

    attr :str
    attr :offset

    #####################################################
    #
    # initialize
    #
    #####################################################
    def initialize(str,offset)

      @str=str
      @offset=offset

    end

    #####################################################
    #
    # to_s
    #
    #####################################################
    def to_s(cycle)

      # Calculate the reference time
      reftime = cycle.gmtime+@offset

      # Compute non-standard date/time components
      days_in_month = Date.new(reftime.year, reftime.month, -1).day
      lower_case_month_abbr = reftime.strftime("%b").downcase
      lower_case_month_full = reftime.strftime("%B").downcase

      # Take care of non-standard flags first
      str = @str.gsub("%n","#{days_in_month}")
      str = str.gsub("%o","#{lower_case_month_abbr}")
      str = str.gsub("%O","#{lower_case_month_full}")

      # Process standard flags
      str = reftime.strftime(str)

      return (str)

    end


    #####################################################
    #
    # hash
    #
    #####################################################
    def hash

      @str.hash ^ @offset.hash

    end


    #####################################################
    #
    # inspect
    #
    #####################################################
    def inspect

      if @offset
          return "<cyclestr offset=\"#{@offset}\">#{@str}</cyclestr>"
      else
          return "<cyclestr>#{@str}</cyclestr>"
      end

    end


    #####################################################
    #
    # eql?
    #
    #####################################################
    def eql?(other)

      return @str==other.str && @offset==other.offset

    end

  end

end
