unless defined? $__cyclestring__

if File.symlink?(__FILE__)
  $:.unshift(File.dirname(File.readlink(__FILE__))) unless $:.include?(File.dirname(File.readlink(__FILE__))) 
else
  $:.unshift(File.dirname(__FILE__)) unless $:.include?(File.dirname(__FILE__)) 
end
$:.unshift("#{File.dirname(__FILE__)}/libxml-ruby-0.8.3/ext/libxml")

##########################################
#
# Class CycleString
#
##########################################
class CycleString

  require 'cycletime.rb'

  #####################################################
  #
  # initialize
  #
  #####################################################
  def initialize(element)

    @strarray=element.collect {|e|
      if e.is_a?(String)
        e
      elsif e.node_type==LibXML::XML::Node::TEXT_NODE
        e.content
      else
        offset_str=e.attributes["offset"]
        offset_sec=0
        unless offset_str.nil?
          offset_sign=offset_str[/^-/].nil? ? 1 : -1
          offset_str.split(":").reverse.each_with_index {|i,index| 
            if index==3
              offset_sec+=i.to_i.abs*3600*24
            elsif index < 3
              offset_sec+=i.to_i.abs*60**index
            else
              raise "Invalid offset, '#{offset_str}' inside of #{e}"
            end           
          }
          offset_sec*=offset_sign
        end

        case e.name
          when "cycle_Y"
            Cycle_Y.new(offset_sec)
          when "cycle_y"
            Cycle_y.new(offset_sec)
          when "cycle_j"
            Cycle_j.new(offset_sec)
          when "cycle_m"
            Cycle_m.new(offset_sec)
          when "cycle_d"
            Cycle_d.new(offset_sec)
          when "cycle_H"
            Cycle_H.new(offset_sec)
          when "cycle_M"
            Cycle_M.new(offset_sec)
          when "cycle_S"
            Cycle_S.new(offset_sec)
          else
            raise "Invalid tag <#{e.name}> inside #{element}"
        end
      end
    } 

  end


  #####################################################
  #
  # to_s
  #
  #####################################################
  def to_s(cycle)

    @strarray.collect {|obj|
      if obj.is_a?(String)
        obj
      elsif obj.is_a?(CycleTime)
        obj.to_s(cycle.gmtime)
      end
    }.to_s

  end


end

$__cyclestring__ == __FILE__
end
