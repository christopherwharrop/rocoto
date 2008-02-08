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
  #####################################################
  def first

    return Time.gm(@year==["*"]   ? 0 : @year[0],
                   @month==["*"]  ? 1 : @month[0],
                   @day==["*"]    ? 1 : @day[0],
                   @hour==["*"]   ? 0 : @hour[0],
                   @minute==["*"] ? 0 : @minute[0])    

  end


  #####################################################
  #
  # last
  #
  #####################################################
  def last

    if @year==["*"]
      years=(0..9999).to_a
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
    years.reverse.each { |yr|
      months.reverse.each { |mo|
        days.reverse.each { |dy|
          if Date.valid_civil?(yr,mo,dy)
            return Time.gm(yr,mo,dy,@hour==["*"] ? 23 : @hour.last,@minute==["*"] ? 59 : @minute.last)
          end
        }
      }
    }

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
