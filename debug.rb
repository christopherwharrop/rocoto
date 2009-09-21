unless defined? $__debug__

if File.symlink?(__FILE__)
  $:.unshift(File.dirname(File.readlink(__FILE__))) unless $:.include?(File.dirname(File.readlink(__FILE__)))
else
  $:.unshift(File.dirname(__FILE__)) unless $:.include?(File.dirname(__FILE__))
end

module Debug

  def Debug.message(message,level)

    if ENV["__WFM_VERBOSE__"].to_i >= level     
      printf "%s  DEBUG %03d: %s\n",Time.now.strftime("%b %d %Y %H:%M:%S"),ENV["__WFM_VERBOSE__"],message
    end

  end

end

$__debug__ == __FILE__
end
