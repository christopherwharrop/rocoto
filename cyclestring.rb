unless defined? $__cyclestring__

if File.symlink?(__FILE__)
  $:.unshift(File.dirname(File.readlink(__FILE__))) unless $:.include?(File.dirname(File.readlink(__FILE__))) 
else
  $:.unshift(File.dirname(__FILE__)) unless $:.include?(File.dirname(__FILE__)) 
end
$:.unshift("#{File.dirname(__FILE__)}/usr/lib64/ruby/site_ruby/1.8/x86_64-linux") 

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
      if e.node_type==LibXML::XML::Node::TEXT_NODE
#      if e.kind_of?(REXML::Text)
        e.content
      else
        offset=e.attributes["offset"].to_i
        case e.name
          when "cycle_Y"
            Cycle_Y.new(offset)
          when "cycle_y"
            Cycle_y.new(offset)
          when "cycle_j"
            Cycle_j.new(offset)
          when "cycle_m"
            Cycle_m.new(offset)
          when "cycle_d"
            Cycle_d.new(offset)
          when "cycle_H"
            Cycle_H.new(offset)
          when "cycle_M"
            Cycle_M.new(offset)
          when "cycle_S"
            Cycle_S.new(offset)
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
