/* $Id: ruby_xml_parser_context.h 319 2008-07-08 07:40:27Z cfis $ */

/* Please see the LICENSE file for copyright and distribution information */

#ifndef __RUBY_XML_PARSER_CONTEXT__
#define __RUBY_XML_PARSER_CONTEXT__

extern VALUE cXMLParserContext;

typedef struct ruby_xml_parser_context {
  xmlParserCtxtPtr ctxt;
  int is_ptr;
} ruby_xml_parser_context;

void ruby_xml_parser_context_free(ruby_xml_parser_context *ctxt);
void ruby_init_xml_parser_context(void);
VALUE ruby_xml_parser_context_new();
VALUE ruby_xml_parser_context_each(VALUE self);

#endif
