#!/usr/bin/ruby

require '/whome/harrop/workflowmgr_dev/lib/workflowmgr/forkit.rb'

#filename='/lfs1/jetmgmt/harrop/test/x'
#filename='/pan1/jetmgmt/harrop/test/x'
filename='/home/harrop/test/x'

# Open and write to a file
result=forkit(2) do
  File.open(filename,"w") do |file|
    file.puts("hello from #{Process.pid}")
  end
end
puts result.inspect

exit

# File exists?
result=forkit(2) do
  File.exists?(filename)
end
puts result

# File mtime
if result
  result=forkit(2) do
    File.mtime(filename)
  end
  puts result
end

# Performance test of forking
10.times do
  result=forkit(1) do
    File.exists?(filename)
  end
end