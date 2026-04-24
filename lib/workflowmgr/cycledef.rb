##########################################
#
# Module WorkflowMgr
#
##########################################
module WorkflowMgr
  ##########################################
  #
  # Class CycleDef
  #
  ##########################################
  class CycleDef
    attr_reader :group, :cycledef, :position, :activation_offset

    ##########################################
    #
    # seek
    #
    ##########################################
    def seek(reftime)
      @position = reftime
    end

    ##########################################
    #
    # first
    #
    ##########################################
    def first
      self.next(Time.gm(1900, 1, 1, 0, 0), by_activation_time: false)[0]
    end

    ##########################################
    #
    # last
    #
    ##########################################
    def last
      previous(Time.gm(9999, 12, 31, 23, 59), by_activation_time: false)[0]
    end

    ##########################################
    #
    # each
    #
    ##########################################
    def each(rawreftime, by_activation_time)
      now = rawreftime
      until now.nil?
        ret = self.next(now - 1, by_activation_time: by_activation_time)
        return if ret.nil?

        yield ret[0]
        now = ret[0] + 2
      end
    end
  end

  ##########################################
  #
  # Class CycleCron
  #
  ##########################################
  class CycleCron < CycleDef
    require 'date'

    ##########################################
    #
    # initialize
    #
    ##########################################
    def initialize(cycledef, group, activation_offset, position = nil)
      super()

      @cycledef = cycledef
      @group = group
      @activation_offset = activation_offset

      @fields = {}
      [:minute, :hour, :day, :month, :year, :weekday].each_with_index do |field, i|
        @fields[field] = cronstr_to_a(@cycledef.split[i], field)
      end

      @position = position || first
    end

    ##########################################
    #
    # next
    #
    ##########################################
    def next(rawreftime, by_activation_time: false)
      # Take the activation offset into account
      reftime = if by_activation_time
                  rawreftime - @activation_offset
                else
                  rawreftime
                end

      # Get date/time components for the reference time
      nextmin = reftime.min
      nexthr = reftime.hour
      nextday = reftime.day
      nextwday = reftime.wday
      nextmonth = reftime.month
      nextyear = reftime.year

      # Find the first minute >= ref minute, carry over to next hour if none found
      min = @fields[:minute].find { |minute| minute >= nextmin }
      if min.nil?
        min = @fields[:minute].first
        nexthr += 1
      end
      nextmin = min

      # Find the first hour >= ref hour, carry over to next day
      hr = @fields[:hour].find { |hour| hour >= nexthr }
      if hr.nil?
        hr = @fields[:hour].first
        nextwday = (nextwday + 1) % 7
        nextday += 1
        until Date.valid_civil?(nextyear, nextmonth, nextday)
          nextday += 1
          next unless nextday > 31

          nextday = 1
          nextmonth += 1
          if nextmonth > 12
            nextmonth = 1
            nextyear += 1
          end
        end
      end
      if nexthr != hr
        nextmin = @fields[:minute].first
      end
      nexthr = hr

      # Check if all days are specified in the cronspec
      alldays = @fields[:day] == get_field_range(:day).to_a

      # Check if all weekdays are specified in the cronspec
      allweekdays = @fields[:weekday] == get_field_range(:weekday).to_a

      # Find the next valid year,month,day
      loop do
        # Set done to true
        done = true

        # If day spec is *, then find the next wday
        if alldays

          # Find the first weekday >= ref weekday, add that many days to reftime
          wday = @fields[:weekday].find { |weekday| weekday >= nextwday }
          ndays = if wday.nil?
                    @fields[:weekday].first - nextwday + 7
                  else
                    wday - nextwday
                  end
          if ndays > 0
            nextmin = @fields[:minute].first
            nexthr = @fields[:hour].first
            done = false
          end
          nexttime = Time.gm(nextyear, nextmonth, nextday) + (24 * 3600 * ndays)
          nextday = nexttime.day
          nextwday = nexttime.wday
          nextmonth = nexttime.month
          nextyear = nexttime.year

        # If weekday spec is *, and day spec is not *, find next day
        elsif allweekdays

          # Find the first day >= ref day, carry over to next month
          day = @fields[:day].find { |day| day >= nextday }

          if day.nil?
            day = @fields[:day].first
            nextmonth += 1
          end
          if nextday != day
            done = false
            nextmin = @fields[:minute].first
            nexthr = @fields[:hour].first
          end
          nextday = day

        # If neither weekday nor day spec is *, find next day that satisfies one or the other
        else

          # Add one day until either days or weekdays is satisfied
          # Inefficient, but will loop no more than 6 times
          while !@fields[:day].member?(nextday) && !@fields[:weekday].member?(nextwday)
            done = false
            nexttime = Time.gm(nextyear, nextmonth, nextday) + (24 * 3600)
            nextmin = @fields[:minute].first
            nexthr = @fields[:hour].first
            nextday = nexttime.day
            nextwday = nexttime.wday
            nextmonth = nexttime.month
            nextyear = nexttime.year
          end

        end

        # Find the first month >= ref month, carry over to next year
        month = @fields[:month].find { |month| month >= nextmonth }
        if month.nil?
          month = @fields[:month].first
          nextyear += 1
        end
        if nextmonth != month
          done = false
          nextmin = @fields[:minute].first
          nexthr = @fields[:hour].first
          nextday = 1
        end
        nextmonth = month

        # Find the first year >= ref year, carry over to next year
        year = @fields[:year].find { |year| year >= nextyear }
        if year.nil?
          return nil # There is no date in the cron spec >= to the reftime
        end

        if nextyear != year
          done = false
          nextmin = @fields[:minute].first
          nexthr = @fields[:hour].first
          nextday = 1
          nextmonth = @fields[:month].first
        end
        nextyear = year

        until Date.valid_civil?(nextyear, nextmonth, nextday)
          done = false
          nextday += 1
          next unless nextday > 31

          nextday = 1
          nextmonth += 1
          if nextmonth > 12
            nextmonth = 1
            nextyear += 1
          end
        end
        if done
          return Time.gm(nextyear, nextmonth, nextday, nexthr,
                         nextmin), Time.gm(nextyear, nextmonth, nextday, nexthr, nextmin) + @activation_offset
        end

        nextwday = Time.gm(nextyear, nextmonth, nextday).wday
      end
    end

    ##########################################
    #
    # previous
    #
    ##########################################
    def previous(rawreftime, by_activation_time: false)
      # Take the activation offset into account
      reftime = by_activation_time ? rawreftime - @activation_offset : rawreftime

      # Get date/time components for the reference time
      prevmin = reftime.min
      prevhr = reftime.hour
      prevday = reftime.day
      prevwday = reftime.wday
      prevmonth = reftime.month
      prevyear = reftime.year

      # Find the last minute <= ref minute, carry over to prev hour if none found
      min = @fields[:minute].reverse.find { |minute| minute <= prevmin }
      if min.nil?
        min = @fields[:minute].last
        prevhr -= 1
      end
      prevmin = min

      # Find the last hour <= ref hour, carry over to prev day
      hr = @fields[:hour].reverse.find { |hour| hour <= prevhr }
      if hr.nil?
        hr = @fields[:hour].last
        prevwday = (prevwday + 6) % 7
        prevday -= 1
        until Date.valid_civil?(prevyear, prevmonth, prevday)
          prevday -= 1
          next unless prevday < 1

          prevday = 31
          prevmonth -= 1
          if prevmonth < 1
            prevmonth = 12
            prevyear -= 1
          end
        end
      end
      if prevhr != hr
        prevmin = @fields[:minute].last
      end
      prevhr = hr

      # Check if all days are specified in the cronspec
      alldays = @fields[:day] == get_field_range(:day).to_a

      # Check if all weekdays are specified in the cronspec
      allweekdays = @fields[:weekday] == get_field_range(:weekday).to_a

      # Find the prev valid year,month,day
      loop do
        # Set done to true
        done = true

        # If day spec is *, then find the prev wday
        if alldays

          # Find the first weekday <= ref weekday, subtract that many days from reftime
          wday = @fields[:weekday].reverse.find { |weekday| weekday <= prevwday }
          ndays = if wday.nil?
                    prevwday - @fields[:weekday].first + 7
                  else
                    prevwday - wday
                  end
          if ndays > 0
            prevmin = @fields[:minute].last
            prevhr = @fields[:hour].last
            done = false
          end

          prevtime = Time.gm(prevyear, prevmonth, prevday) - (24 * 3600 * ndays)
          prevday = prevtime.day
          prevwday = prevtime.wday
          prevmonth = prevtime.month
          prevyear = prevtime.year

        # If weekday spec is *, and day spec is not *, find prev day
        elsif allweekdays

          # Find the first day <= ref day, carry over to prev month
          day = @fields[:day].reverse.find { |day| day <= prevday }

          if day.nil?
            day = @fields[:day].last
            prevmonth -= 1
          end
          if prevday != day
            done = false
            prevmin = @fields[:minute].last
            prevhr = @fields[:hour].last
          end
          prevday = day

        # If neither weekday nor day spec is *, find prev day that satisfies one or the other
        else

          # Add one day until either days or weekdays is satisfied
          # Inefficient, but will loop no more than 6 times
          while !@fields[:day].member?(prevday) && !@fields[:weekday].member?(prevwday)
            done = false
            prevtime = Time.gm(prevyear, prevmonth, prevday) - (24 * 3600)
            prevmin = @fields[:minute].last
            prevhr = @fields[:hour].last
            prevday = prevtime.day
            prevwday = prevtime.wday
            prevmonth = prevtime.month
            prevyear = prevtime.year
          end

        end

        # Find the first month <= ref month, carry over to prev year
        month = @fields[:month].reverse.find { |month| month <= prevmonth }
        if month.nil?
          month = @fields[:month].last
          prevyear -= 1
        end
        if prevmonth != month
          done = false
          prevmin = @fields[:minute].last
          prevhr = @fields[:hour].last
          prevday = 31
        end
        prevmonth = month

        # Find the first year <= ref year, carry over to prev year
        year = @fields[:year].reverse.find { |year| year <= prevyear }
        if year.nil?
          return nil # There is no date in the cron spec <= to the reftime
        end

        if prevyear != year
          done = false
          prevmin = @fields[:minute].last
          prevhr = @fields[:hour].last
          prevday = 31
          prevmonth = @fields[:month].last
        end
        prevyear = year

        until Date.valid_civil?(prevyear, prevmonth, prevday)
          done = false
          prevday -= 1
          next unless prevday < 1

          prevday = 31
          prevmonth -= 1
          if prevmonth < 1
            prevmonth = 12
            prevyear -= 1
          end
        end

        if done
          return Time.gm(prevyear, prevmonth, prevday, prevhr,
                         prevmin), Time.gm(prevyear, prevmonth, prevday, prevhr, prevmin) + @activation_offset
        end

        prevwday = Time.gm(prevyear, prevmonth, prevday).wday
      end
    end

    ##########################################
    #
    # member?
    #
    ##########################################
    def member?(reftime)
      gmreftime = reftime.getgm
      if @fields[:month].member?(gmreftime.month) && @fields[:hour].member?(gmreftime.hour)

        # Check if all days are specified in the cronspec
        alldays = @fields[:day] == get_field_range(:day).to_a

        # Check if all weekdays are specified in the cronspec
        allweekdays = @fields[:weekday] == get_field_range(:weekday).to_a
        daymember = if alldays
                      @fields[:weekday].member?(gmreftime.wday)
                    elsif allweekdays
                      @fields[:day].member?(gmreftime.day)
                    else
                      @fields[:day].member?(gmreftime.day) || @fields[:weekday].member?(gmreftime.wday)
                    end

        if daymember && @fields[:minute].member?(gmreftime.min) && @fields[:year].member?(gmreftime.year)
          return true
        end
      end

      false
    end


    private

    ##########################################
    #
    # cronstr_to_a
    #
    ##########################################
    def cronstr_to_a(str, field)
      field_range = get_field_range(field)
      if str =~ /^\*$/
        field_range.to_a
      elsif str =~ /^\*\/(\d+)$/
        step = ::Regexp.last_match(1).to_i
        field_range.find_all { |i| (i - field_range.first) % step == 0 }
      else
        cronarr = str.split(",").collect! do |i|
          case i
          when /^\d+$/
            i.to_i
          when /^(\d+)-(\d+)$/
            (::Regexp.last_match(1).to_i..::Regexp.last_match(2).to_i).to_a
          when /^(\d+)-(\d+)\/(\d+)$/
            a = ::Regexp.last_match(1).to_i
            b = ::Regexp.last_match(2).to_i
            c = ::Regexp.last_match(3).to_i
            (a..b).find_all { |i| (i - a) % c == 0 }
          else
            raise "Illegal Cycle cron field, '#{str}'"
          end
        end.flatten.sort.uniq
        if cronarr & field_range.to_a != cronarr
          raise "Illegal Cycle cron field, '#{str}'"
        end

        cronarr

      end
    end

    ##########################################
    #
    # get_field_range
    #
    ##########################################
    def get_field_range(field)
      case field
      when :minute
        (0..59)
      when :hour
        (0..23)
      when :day
        (1..31)
      when :month
        (1..12)
      when :year
        (1900..9999)
      when :weekday
        (0..6)
      else
        raise "Unsupported cycle cron field '#{field}'"
      end
    end
  end

  ##########################################
  #
  # Class CycleInterval
  #
  ##########################################
  class CycleInterval < CycleDef
    require 'workflowmgr/utilities'

    attr_reader :exclude_hours, :valid_hours

    ##########################################
    #
    # initialize
    #
    ##########################################
    def initialize(cycledef, group, activation_offset = 0, position = nil, exclude_hours = nil, valid_hours = nil)
      super()

      @cycledef = cycledef
      @group = group
      @activation_offset = activation_offset

      # Parse exclude_hours - hours to exclude from the cycle
      @exclude_hours = parse_hours(exclude_hours)

      # Parse valid_hours - only these hours are valid in the cycle
      @valid_hours = parse_hours(valid_hours)

      # Validate that exclude_hours and valid_hours are not both specified
      if @exclude_hours && @valid_hours
        raise "Invalid <cycledef>: Cannot specify both exclude_hours and valid_hours"
      end

      fields = @cycledef.split
      @start = Time.gm(fields[0][0..3],
                       fields[0][4..5],
                       fields[0][6..7],
                       fields[0][8..9],
                       fields[0][10..11])
      @finish = if fields[1] =~ /\d{12}/
                  Time.gm(fields[1][0..3],
                          fields[1][4..5],
                          fields[1][6..7],
                          fields[1][8..9],
                          fields[1][10..11])
                else
                  @start + WorkflowMgr.ddhhmmss_to_seconds(fields[1])
                end

      raise "Invalid <cycledef>  Start time is greater than the end time" if @start > @finish

      @interval = WorkflowMgr.ddhhmmss_to_seconds(fields[2])

      raise "Invalid <cycledef>  Interval must be a positive unit of time" if @interval == 0

      @position = position || first
    end

    ##########################################
    #
    # parse_hours
    #
    ##########################################
    def parse_hours(hours_str)
      return nil if hours_str.nil? || hours_str.strip.empty?

      hours = hours_str.strip.split.map(&:to_i)

      # Validate that all hours are in the range 0-23
      hours.each do |hour|
        unless hour >= 0 && hour <= 23
          raise "Invalid hour value '#{hour}' in cycledef. Hours must be in the range 0-23."
        end
      end

      hours.uniq.sort
    end

    ##########################################
    #
    # hour_valid?
    #
    ##########################################
    def hour_valid?(time)
      hour = time.getgm.hour # always use UTC hour

      if @valid_hours
        # Only valid_hours are valid
        @valid_hours.include?(hour)
      elsif @exclude_hours
        # All hours except exclude_hours are valid
        !@exclude_hours.include?(hour)
      else
        # No filtering, all hours are valid
        true
      end
    end

    ##########################################
    #
    # next
    #
    ##########################################
    def next(rawreftime, by_activation_time: false)
      # Take the activation offset into account
      reftime = by_activation_time ? rawreftime - @activation_offset : rawreftime

      if reftime > @finish
        return nil, nil
      elsif reftime <= @start
        candidate = @start
      else
        offset = (reftime.to_i - @start.to_i) % @interval
        candidate = if offset == 0
                      reftime
                    else
                      Time.at(reftime - offset + @interval)
                    end
      end

      # Apply hour filtering if exclude_hours or valid_hours is specified
      while candidate <= @finish
        if hour_valid?(candidate)
          return candidate.getgm, candidate.getgm + @activation_offset
        end

        candidate = Time.at(candidate.to_i + @interval)
      end

      [nil, nil]
    end

    ##########################################
    #
    # previous
    #
    ##########################################
    def previous(rawreftime, by_activation_time: false)
      # Take the activation offset into account
      reftime = by_activation_time ? rawreftime - @activation_offset : rawreftime

      if reftime < @start
        return nil, nil
      elsif reftime >= @finish
        candidate = @finish
      else
        offset = (reftime.to_i - @start.to_i) % @interval
        candidate = Time.at(reftime - offset)
      end

      # Apply hour filtering if exclude_hours or valid_hours is specified
      while candidate >= @start
        if hour_valid?(candidate)
          return candidate.getgm, candidate.getgm + @activation_offset
        end

        candidate = Time.at(candidate.to_i - @interval)
      end

      [nil, nil]
    end

    ##########################################
    #
    # member?
    #
    ##########################################
    def member?(reftime)
      return false if reftime < @start || reftime > @finish
      return false unless ((reftime.to_i - @start.to_i) % @interval) == 0

      # Apply hour filtering
      hour_valid?(reftime)
    end
  end
end
