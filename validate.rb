#!/usr/bin/ruby

unless defined? $__workflow__

if File.symlink?(__FILE__)
  $:.unshift(File.dirname(File.readlink(__FILE__))) unless $:.include?(File.dirname(File.readlink(__FILE__))) 
else
  $:.unshift(File.dirname(__FILE__)) unless $:.include?(File.dirname(__FILE__)) 
end
$:.unshift("#{File.dirname(__FILE__)}/libxml-ruby/lib")
$:.unshift("#{File.dirname(__FILE__)}/libxml-ruby/ext/libxml")

require 'libxml.rb'

# parse schema as xml document
relaxng_document = LibXML::XML::Document.file('schema.rng')

# prepare schema for validation
relaxng_schema = LibXML::XML::RelaxNG.document(relaxng_document)

# parse xml document to be validated
instance = LibXML::XML::Document.file(ARGV[0],:options => LibXML::XML::Parser::Options::NOENT)

# validate
instance.validate_relaxng(relaxng_schema)

end