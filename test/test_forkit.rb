#!/usr/bin/ruby

require 'forkit'

#filename='/lfs1/jetmgmt/harrop/test/x'
#filename='/pan1/jetmgmt/harrop/test/x'
filename='/home/harrop/test/x'

result=forkit(5) do
  File.exists?(filename)
end
puts result
if result
  result=forkit(5) do
    File.mtime(filename)
  end
  puts result
end
File.open(filename,"w") do |file|
  result=forkit(5) do
    file.puts("hello")
  end
end

100.times do
  result=forkit(5) do
    File.exists?(filename)
  end
end