##########################################
#
# Module WorkflowMgr
#
##########################################
module WorkflowMgr

  ########################################## 
  #
  # Class WorkflowXMLDoc 
  #
  ##########################################
  class WorkflowXMLDoc

    require 'libxml'
    require 'workflowmgr/compoundtimestring'
    require 'workflowmgr/cycleformat'

    ##########################################
    #
    # initialize
    #
    ##########################################
    def initialize(workflowdoc)

      @xmlfile=workflowdoc

      # Get the text from the xml file and put it into a string
      @xmlstring=WorkflowMgr.forkit(2) do
        IO.readlines(@xmlfile,nil)[0]
      end

      # Parse the workflow xml string, set option to replace entities
      @workflowdoc=LibXML::XML::Document.string(@xmlstring,:options => LibXML::XML::Parser::Options::NOENT)

      # Validate the workflow xml document before metatask expansion
      validate_with_metatasks

      # Expand metatasks
      expand_metatasks

      # Validate the workflow xml document after metatask expansion
      # The second validation is needed in case metatask expansion introduced invalid XML
      validate_without_metatasks

    end  # initialize

    ##########################################
    #
    # realtime?
    #
    ##########################################
    def realtime?

      realtime=@workflowdoc.root.attributes['realtime'].downcase =~ /^t|true$/
      !realtime.nil?

    end

    ##########################################
    #
    # cyclethrottle
    #
    ##########################################
    def cyclethrottle

      cyclethrottle=@workflowdoc.root.attributes['cyclethrottle']
      if cyclethrottle.nil?
        return 1
      else
        return cyclethrottle.to_i
      end

    end


    ##########################################
    #
    # cyclelifespan
    #
    ##########################################
    def cyclelifespan

      cyclelifespan=@workflowdoc.root.attributes['cyclelifespan']
      if cyclelifespan.nil?
        return nil
      else
        lifespan=0
        cyclelifespan.split(":").reverse.each_with_index {|i,index|
          if index==3
            lifespan+=i.to_i.abs*3600*24
          elsif index < 3
            lifespan+=i.to_i.abs*60**index
  	  else
            raise "Invalid cycle life span, '#{cyclelifespan}'"
          end
        }
        return lifespan
      end      

    end


    ##########################################
    #
    # scheduler
    #
    ##########################################
    def scheduler

      scheduler=@workflowdoc.root.attributes['scheduler']
      if scheduler.nil?
        return "auto"
      else
        return scheduler.downcase
      end

    end


    ##########################################
    #
    # cycles
    #
    ##########################################
    def cycles

      cycles=[]
      cyclenodes=@workflowdoc.find('/workflow/cycle')
      cyclenodes.each do |cyclenode|
        cycles << { :group => cyclenode.attributes['group'], :fieldstr => cyclenode.content }
      end
      return cycles

    end


    ##########################################
    #
    # tasks
    #
    ##########################################
    def tasks

      tasks=[]
      tasknodes=@workflowdoc.find('/workflow/task')
      tasknodes.each do |tasknode|
        task={}
        tasknode.attributes.each { |attr| task[attr.name.to_sym]=attr.value }
        tasknode.each_element do |e|
          case e.name
            when /^envar$/
              task[e.name.to_sym] = {} if task[e.name.to_sym].nil?
              task[e.name.to_sym][get_compound_time_string(e.find('name').first)] = get_compound_time_string(e.find('value').first)
            when /^dependency$/
            else
              task[e.name.to_sym]=get_compound_time_string(e)
          end
        end
        tasks << task
      end
      return tasks
    end


  private


    ##########################################
    #
    # get_compound_time_string
    # 
    ##########################################
    def get_compound_time_string(element)

      strarray=element.collect { |e|
        if e.node_type==LibXML::XML::Node::TEXT_NODE
          CycleFormat.new(e.content,0)
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
            when "cyclestr"
              formatstr=e.content.gsub(/@(\^?[^@\s])/,'%\1').gsub(/@@/,'@')
              CycleFormat.new(formatstr,offset_sec)
            else
              raise "Invalid tag <#{e.name}> inside #{element}: #{e.node_type_name}"
          end
        end
      } 

      return CompoundTimeString.new(strarray)

    end



    ##########################################
    #
    # validate_with_metatasks
    # 
    ##########################################
    def validate_with_metatasks

      # This method is not wrapped inside a WorkflowMgr.forkit 
      # because it is reading the schemas from the same directory
      # as this source file.  If the schema validation was going to hang,
      # then this code would not be running anyway

      # Parse the Relax NG schema XML document
      relaxng_document = LibXML::XML::Document.file("#{File.dirname(__FILE__)}/schema_with_metatasks.rng")

      # Prepare the Relax NG schemas for validation
      relaxng_schema = LibXML::XML::RelaxNG.document(relaxng_document)

      # Validate the workflow XML file against the general Relax NG Schema that validates metatask tags
      @workflowdoc.validate_relaxng(relaxng_schema)

    end


    ##########################################
    #
    # validate_with_metatasks
    #
    ##########################################
    def validate_without_metatasks

      # This method is not wrapped inside a WorkflowMgr.forkit 
      # because it is reading the schemas from the same directory
      # as this source file.  If the schema validation was going to hang,
      # then this code would not be running anyway

      # Parse the Relax NG schema XML document
      relaxng_document = LibXML::XML::Document.file("#{File.dirname(__FILE__)}/schema_without_metatasks.rng")

      # Prepare the Relax NG schemas for validation
      relaxng_schema = LibXML::XML::RelaxNG.document(relaxng_document)

      # Validate the workflow XML file against the general Relax NG Schema that validates metatask tags
      @workflowdoc.validate_relaxng(relaxng_schema)

    end


    ##########################################
    #
    # validate_with_metatasks
    #
    ##########################################
    def expand_metatasks

    end


  end  # Class WorkflowXMLDoc

end  # Module WorkflowMgr
