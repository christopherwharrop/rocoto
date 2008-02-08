unless defined? $__cycle__

require 'date'

##########################################
#
# Class CycleCron
#
##########################################
class CycleCron

  attr_reader :name
 
  #####################################################
  #
  # initialize
  #
  #####################################################
  def initialize(name,year,month,day,hour,min)

    getarray=Proc.new { |str|
      if str=="*"
        [str]
      else
        str.split(",").collect! { |i|
          case i
            when /^\d+$/
              i.to_i
            when /^(\d+)-(\d+)\/(\d+)$/
              a=$1.to_i
              b=$2.to_i
              c=$3.to_i      
              (a..b).find_all {|i| (i-a)%c==0}
            when /^(\d+)-(\d+)$/
              ($1.to_i..$2.to_i).to_a
          end
        }.flatten.sort.uniq
      end
    }

    @name=name
    @year=getarray.call(year)
    @month=getarray.call(month)
    @day=getarray.call(day)
    @hour=getarray.call(hour)
    @minute=getarray.call(min)

    @first=self.first
    @last=self.last

  end

  #####################################################
  #
  # first
  #
  # Get the first valid date represented by the cycle
  # cron
  #
  #####################################################
  def first

    if @year==["*"]
      years=(1970..9999).to_a
    else
      years=@year
    end
    if @month==["*"]
      months=(1..12).to_a
    else
      months=@month
    end    
    if @day==["*"]
      days=(1..31).to_a
    else
      days=@day
    end

    years.each { |yr|
      months.each { |mo|
        days.each { |dy|
          if Date.valid_civil?(yr,mo,dy)
            return Time.gm(yr,mo,dy,@hour==["*"] ? 0 : @hour.first,@minute==["*"] ? 0 : @minute.first)
          end
        }
      }
    }
    
    # We've tried every combination and none worked, so return nil
    return nil

  end

  #####################################################
  #
  # last
  #
  #####################################################
  def last

    if @year==["*"]
      years=(1970..9999).to_a
    else
      years=@year
    end
    if @month==["*"]
      months=(1..12).to_a
    else
      months=@month
    end    
    if @day==["*"]
      days=(1..31).to_a
    else
      days=@day
    end
    years.reverse_each { |yr|
      months.reverse_each { |mo|
        days.reverse_each { |dy|
          if Date.valid_civil?(yr,mo,dy)
            return Time.gm(yr,mo,dy,@hour==["*"] ? 23 : @hour.last,@minute==["*"] ? 59 : @minute.last)
          end
        }
      }
    }

    # We've tried every combination and none worked, so return nil
    return nil

  end


  #####################################################
  #
  # next
  #
  #####################################################
  def next(time)

    reftime=time
    if reftime < @first
      atime=@first-60
    else
      atime=Time.at(reftime.to_i - reftime.sec + 60)
    end

    until ((@minute==["*"] || @minute.member?(atime.getgm.min))  &&
           (@hour==["*"]   || @hour.member?(atime.getgm.hour))   &&
           (@day==["*"]    || @day.member?(atime.getgm.day))     &&
           (@month==["*"]  || @month.member?(atime.getgm.month)) &&
           (@year==["*"]   || @year.member?(atime.getgm.year)))
      atime+=60
      return nil if atime > @last
    end

    return atime

  end

  #####################################################
  #
  # next2
  #
  #####################################################
  def next2(reftime)

    # Get the time components of the reftime
    refyear=reftime.getgm.year
    refmonth=reftime.getgm.month
    refday=reftime.getgm.day
    refhour=reftime.getgm.hour
    refmin=reftime.getgm.min

    # Convert cron strings into arrays
    if @year==["*"]
      years=(1970..9999).to_a
    else
      years=@year
    end
    if @month==["*"]
      months=(1..12).to_a
    else
      months=@month
    end    
    if @day==["*"]
      days=(1..31).to_a
    else
      days=@day
    end
    if @hour==["*"]
      hours=(0..23).to_a
    else
      hours=@hour
    end
    if @minute==["*"]
      minutes=(0..59).to_a
    else
      minutes=@minute
    end

    # Get the indices corresponding to the reftime
    iyear=years.index(refyear)
    imonth=months.index(refmonth)
    iday=days.index(refday)
    ihour=hours.index(refhour)
    iminute=minutes.index(refmin)

    i=iyear
    j=imonth
    k=iday
    l=ihour
    m=iminute
    done=false
    while i < years.size
      while j < months.size
        while k < days.size
          if Date.valid_civil?(years[i],months[j],days[k])
            while l < hours.size
              while m < minutes.size-1
                m+=1
                if Date.valid_civil?(years[i],months[j],days[k])
                  return(Time.gm(years[i],months[j],days[k],hours[l],minutes[m]))
                end
              end
              l+=1
              m=-1
            end
          else
            l=0
            m=-1
          end
          k+=1
          l=0
        end
        j+=1
        k=0
      end
      i+=1
      j=0
    end

    return nil  

  end

  #####################################################
  #
  # next_new
  #
  #####################################################
  def next_new(reftime)

    # Check if reftime is < @first or > @last
    if reftime < @first
      return @first
    elsif reftime > @last
      return nil
    end
    
    # Get the time components of the reftime
    refyear=reftime.getgm.year
    refmonth=reftime.getgm.month
    refday=reftime.getgm.day
    refhour=reftime.getgm.hour
    refmin=reftime.getgm.min

    # Convert cron strings into arrays
    if @year==["*"]
      years=(1970..9999).to_a
    else
      years=@year
    end
    if @month==["*"]
      months=(1..12).to_a
    else
      months=@month
    end    
    if @day==["*"]
      days=(1..31).to_a
    else
      days=@day
    end
    if @hour==["*"]
      hours=(0..23).to_a
    else
      hours=@hour
    end
    if @minute==["*"]
      minutes=(0..59).to_a
    else
      minutes=@minute
    end

    # Find the first year that is >= refyear
    iyear=(0..years.size-1).to_a.detect { |i| years[i]>=refyear }
    if years[iyear] > refyear
      return self.next_new(Time.gm(years[iyear],months.first,days.first,hours.first,minutes.first))
    end

    # Find the first month that is >= refmonth
    imonth=(0..months.size-1).to_a.detect { |i| months[i]>=refmonth }
    if imonth.nil?
      return self.next_new(Time.gm(years[iyear]+1,months.first,days.first,hours.first,minutes.first))
    elsif months[imonth] > refmonth
      return self.next_new(Time.gm(years[iyear],months[imonth],days.first,hours.first,minutes.first))
    end

    # Find the first day that is >= refday
    iday=(0..days.size-1).to_a.detect { |i| days[i]>=refday }
    if iday.nil?
      return self.next_new(Time.gm(years[iyear],months[imonth]+1,days.first,hours.first,minutes.first))
    elsif days[iday] > refday
      return self.next_new(Time.gm(years[iyear],days[imonth],days.first,hours.first,minutes.first))
    end

puts iyear,years[iyear]


  end

  #####################################################
  #
  # prev
  #
  #####################################################
  def prev(time)

    # Make sure we are comparing times in GMT
    reftime=time
    if reftime > @last
      atime=@last+60
    else
      atime=Time.at(reftime.to_i - reftime.sec-60)
    end

    until ((@minute==["*"] || @minute.member?(atime.getgm.min)) &&
           (@hour==["*"] || @hour.member?(atime.getgm.hour)) &&
           (@day==["*"] || @day.member?(atime.getgm.day)) &&
           (@month==["*"] || @month.member?(atime.getgm.month)) &&
           (@year==["*"] || @year.member?(atime.getgm.year)))
      atime-=60
      return nil if atime < @first
    end

    return atime

  end

  #####################################################
  #
  # has_cycle?
  #
  #####################################################
  def has_cycle?(cycle)

    refcycle=cycle.getgm
    return ((@minute==["*"] || @minute.member?(refcycle.min))  &&
            (@hour==["*"]   || @hour.member?(refcycle.hour))   &&
            (@day==["*"]    || @day.member?(refcycle.day))     &&
            (@month==["*"]  || @month.member?(refcycle.month)) &&
            (@year==["*"]   || @year.member?(refcycle.year))
           )

  end


  #####################################################
  #
  # all
  #
  #####################################################
  def all
   
    # Expand * for each field
    if @year==["*"]
      raise "ERROR!  Cannot expand cycles when year='*'"
    else
      year=@year
    end
    if @month==["*"]
      month=(1..12).to_a
    else
      month=@month
    end    
    if @day==["*"]
      day=(1..31).to_a
    else
      day=@day
    end
    if @hour==["*"]
      hour=(0..23).to_a
    else
      hour=@hour
    end
    if @minute==["*"]
      minute=(0..59).to_a
    else
      minute=@minute
    end
    if @second==["*"]
      second=(0..59).to_a
    else
      second=@second
    end

    # Loop over all permutations, eliminate invalid combinations
    cycles=Array.new 
    year.each { |yr|      
      month.each {|mo|
        day.each {|dy|
          hour.each {|hr|
            minute.each {|min|
              second.each {|sec|
                if Date.valid_civil?(yr,mo,dy)
                  cycles << Time.gm(yr,mo,dy,hr,min,sec)
                end
              }
            }
          }
        }
      }
    }

    return cycles

  end

end

$__cycle__ == __FILE__
end
