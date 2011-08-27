##########################################
#
# Module WorkflowMgr
#
##########################################
module WorkflowMgr

  ########################################## 
  #
  # Class CycleCron
  #
  ##########################################
  class CycleCron

    require 'date'

    ##########################################
    #
    # initialize
    #
    ##########################################
    def initialize(group,fields)

      @group=group
      @fields={}
      [:minute,:hour,:day,:month,:year,:weekday].each_with_index { |field,i|
        @fields[field]=cronstr_to_a(fields[i],field)
      }

    end  # initialize


    ##########################################
    #
    # first
    #
    ##########################################
    def first

      self.next(Time.gm(999,1,1,0,0))

    end  # first


    ##########################################
    #
    # next
    #
    ##########################################
    def next(reftime)

      # Get date/time components for the reference time
      nextmin=reftime.min
      nexthr=reftime.hour
      nextday=reftime.day
      nextwday=reftime.wday
      nextmonth=reftime.month
      nextyear=reftime.year
    
      # Find the first minute >= ref minute, carry over to next hour if none found
      min=@fields[:minute].find { |minute| minute >= nextmin }
      if min.nil?
        min=@fields[:minute].first
        nexthr+=1
      end
      nextmin=min
#puts "nextmin :: #{nextmin}"
      # Find the first hour >= ref hour, carry over to next day 
      hr=@fields[:hour].find { |hour| hour >= nexthr }
      if hr.nil?
        hr=@fields[:hour].first
        nextday+=1
        nextwday = (nextwday + 1) % 7
      end
      if nexthr != hr
        nextmin=@fields[:minute].first
      end
      nexthr = hr
#puts "nextmin :: #{nextmin}"
#puts "nexthr :: #{nexthr}"
#puts "nextday :: #{nextday}"

      # Check if all days are specified in the cronspec
      alldays=@fields[:day] == get_field_range(:day).to_a

      # Check if all weekdays are specified in the cronspec
      allweekdays=@fields[:weekday] == get_field_range(:weekday).to_a

      # Find the next valid year,month,day
      while (true) do

        # Set done to true
        done=true

        # If day spec is *, then find the next wday
        if alldays

          # Find the first weekday >= ref weekday, add that many days to reftime
          wday=@fields[:weekday].find { |weekday| weekday >= nextwday }
          if wday.nil?
            ndays=@fields[:weekday].first - nextwday + 7
          else
            ndays=wday - nextwday
          end
          if ndays > 0
            nextmin=@fields[:minute].first
            nexthr=@fields[:hour].first
            done=false
          end
          nexttime=Time.gm(nextyear,nextmonth,nextday) + (24 * 3600 * ndays)
          nextday=nexttime.day
          nextwday=nexttime.wday
          nextmonth=nexttime.month
          nextyear=nexttime.year
 
        # If weekday spec is *, and day spec is not *, find next day
        elsif allweekdays

          # Find the first day >= ref day, carry over to next month
          day=@fields[:day].find { |day| day >= nextday }

          if day.nil?
            day=@fields[:day].first
            nextmonth+=1
          end
          if nextday != day
            done=false
            nextmin=@fields[:minute].first
            nexthr=@fields[:hour].first
          end
          nextday=day

        # If neither weekday nor day spec is *, find next day that satisfies one or the other
        else          

          # Add one day until either days or weekdays is satisfied
          # Inefficient, but will loop no more than 6 times
          while !@fields[:day].member?(nextday) && !@fields[:weekday].member?(nextwday) do
            done=false
#puts "nextyear :: #{nextyear}"
#puts "nextmonth :: #{nextmonth}"
#puts "nextday :: #{nextday}"
            nexttime=Time.gm(nextyear,nextmonth,nextday) + (24 * 3600)
            nextmin=@fields[:minute].first
            nexthr=@fields[:hour].first
            nextday=nexttime.day
            nextwday=nexttime.wday
            nextmonth=nexttime.month
            nextyear=nexttime.year
          end

        end
#puts "nextmin :: #{nextmin}"
#puts "nexthr :: #{nexthr}"
#puts "nextday :: #{nextday}"

        # Find the first month >= ref month, carry over to next year
        month=@fields[:month].find { |month| month >= nextmonth }
        if month.nil?
          month=@fields[:month].first
          nextyear+=1
        end
        if nextmonth != month
          done=false
          nextmin=@fields[:minute].first
          nexthr=@fields[:hour].first
          nextday=1
        end
        nextmonth=month
#puts "nextmin :: #{nextmin}"
#puts "nexthr :: #{nexthr}"
#puts "nextday :: #{nextday}"
#puts "nextmonth :: #{nextmonth}"
        
        # Find the first year >= ref year, carry over to next year
        year=@fields[:year].find { |year| year >= nextyear }
        if year.nil?
          return nil  # There is no date in the cron spec >= to the reftime 
        end
        if nextyear != year
          done=false
          nextmin=@fields[:minute].first
          nexthr=@fields[:hour].first
          nextday=1
          nextmonth=@fields[:month].first
        end
        nextyear=year
#puts "nextmin :: #{nextmin}"
#puts "nexthr :: #{nexthr}"
#puts "nextday :: #{nextday}"
#puts "nextmonth :: #{nextmonth}"
#puts "nextyear :: #{nextyear}"

#puts "valid_civil(#{Date.valid_civil?(nextyear,nextmonth,nextday).inspect}) :: #{Time.gm(nextyear,nextmonth,nextday,nexthr,nextmin)} :: #{done}"

        if Date.valid_civil?(nextyear,nextmonth,nextday)
          return Time.gm(nextyear,nextmonth,nextday,nexthr,nextmin) if done
          nextwday=Time.gm(nextyear,nextmonth,nextday).wday
        else
          nextday += 1
          if nextday > 31
            nextday=1
            nextmonth +=1
            if nextmonth > 12
              nextmonth=1
              nextyear += 1
            end
          end
        end

      end  #  while true

    end  #  next
      
  private

    ##########################################
    #
    # cronstr_to_a
    #
    ##########################################
    def cronstr_to_a(str,field)

      field_range=get_field_range(field)
      if str =~ /^\*$/
        field_range.to_a
      elsif str =~ /^\*\/(\d+)$/
        step=$1.to_i
        field_range.find_all {|i| (i-field_range.first) % step == 0}
      else
        cronarr=str.split(",").collect! { |i|
          case i
            when /^\d+$/
              i.to_i
            when /^(\d+)-(\d+)$/
              ($1.to_i..$2.to_i).to_a
            when /^(\d+)-(\d+)\/(\d+)$/
              a=$1.to_i
              b=$2.to_i
              c=$3.to_i      
              (a..b).find_all {|i| (i-a)%c==0}
            else
	      raise "Illegal Cycle cron field, '#{str}'"
          end
        }.flatten.sort.uniq
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
          (1970..2099)
        when :weekday
          (0..6)
        else
	  raise "Unsupported cycle cron field '#{field}'"
      end

    end

  end  # Class CycleCron



  ########################################## 
  #
  # Class CycleInterval
  #
  ##########################################
  class CycleInterval

    ##########################################
    #
    # initialize
    #
    ##########################################
    def initialize(group,fields)

      @group=group
      @start=Time.gm(fields[0][0..3],
                     fields[0][4..5],
                     fields[0][6..7],
                     fields[0][8..9],
                     fields[0][10..11])
      @finish=Time.gm(fields[1][0..3],
                      fields[1][4..5],
                      fields[1][6..7],
                      fields[1][8..9],
                      fields[1][10..11])

      @interval=0
      fields[2].split(":").reverse.each_with_index {|i,index| 
        if index==3
          @interval+=i.to_i.abs*3600*24
        elsif index < 3
          @interval+=i.to_i.abs*60**index
        else
          raise "Invalid cycle interval, '#{fields[2]}'"
        end           
      }

    end  # initialize

    ##########################################
    #
    # first
    #
    ##########################################
    def first

      return @start.gmtime

    end  # first


    ##########################################
    #
    # next
    #
    ##########################################
    def next(reftime)

      localnext=Time.at(reftime - ((reftime.to_i - @start.to_i) % @interval) + @interval)
      return localnext - localnext.gmt_offset

    end  # next



  end  # Class CycleInterval

end  # Module WorkflowMgr