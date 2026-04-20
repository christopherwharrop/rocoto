# Migration Guide: libxml-ruby to Nokogiri

## Key API Differences

### Parsing
```ruby
# libxml-ruby
context = LibXML::XML::Parser::Context.file(xmlfile)
context.options = LibXML::XML::Parser::Options::NOENT | 
                  LibXML::XML::Parser::Options::HUGE | 
                  LibXML::XML::Parser::Options::NOCDATA
parser = LibXML::XML::Parser.new(context)
doc = parser.parse

# Nokogiri
doc = Nokogiri::XML(File.read(xmlfile)) do |config|
  config.noent    # Expand entities
  config.huge     # Allow huge documents
  config.nocdata  # Convert CDATA to text nodes
end
```

### RelaxNG Validation
```ruby
# libxml-ruby
relaxng_document = LibXML::XML::Parser.string(xmlstring, 
  options: LibXML::XML::Parser::Options::NOENT).parse
relaxng_schema = LibXML::XML::RelaxNG.document(relaxng_document)
doc.validate_relaxng(relaxng_schema)  # Returns true/raises exception

# Nokogiri
schema = Nokogiri::XML::RelaxNG(xmlstring)
errors = schema.validate(doc)  # Returns array of errors (empty if valid)
raise "Validation failed: #{errors.join(', ')}" unless errors.empty?
```

### XPath Queries
```ruby
# libxml-ruby
nodes = doc.find('//task')
node = doc.find('//log').first

# Nokogiri (very similar)
nodes = doc.xpath('//task')
node = doc.at_xpath('//log')
```

### Attributes
```ruby
# libxml-ruby
value = node.attributes['name']
node.attributes.each { |attr| ... }

# Nokogiri (nearly identical)
value = node['name']  # or node.attribute('name').value
node.attributes.each { |name, attr| ... }
```

### Node Creation
```ruby
# libxml-ruby
newnode = LibXML::XML::Node.new("taskdep")
LibXML::XML::Attr.new(newnode, "task", task_name)

# Nokogiri
newnode = Nokogiri::XML::Node.new("taskdep", doc)
newnode['task'] = task_name
```

### Node Types
```ruby
# libxml-ruby
if node.node_type == LibXML::XML::Node::TEXT_NODE
if node.node_type == LibXML::XML::Node::COMMENT_NODE

# Nokogiri
if node.text?
if node.comment?
```

### Output Escaping
```ruby
# libxml-ruby
node.output_escaping = false

# Nokogiri
# Not needed - Nokogiri handles this differently
# Use node.content instead of node.text if you need raw content
```

## Migration Steps

1. Update `require 'libxml'` → `require 'nokogiri'`
2. Replace parsing code in initialize method
3. Update validation methods (validate_with_metatasks, validate_without_metatasks)
4. Replace XPath calls: `doc.find()` → `doc.xpath()`
5. Update node type checks
6. Test thoroughly with existing workflow files

## Benefits
- ✅ No libxml2-dev system dependency required
- ✅ Precompiled binaries for common platforms
- ✅ Actively maintained (libxml2 bundled and updated)
- ✅ Better documentation and community support
- ✅ Compatible API with minor changes
