/* $Id: ruby_xml_xinclude.c 461 2008-07-15 21:35:56Z cfis $ */

/* Please see the LICENSE file for copyright and distribution information */

#include "ruby_libxml.h"
#include "ruby_xml_xinclude.h"

VALUE cXMLXInclude;
VALUE eXMLXIncludeError;

// Rdoc needs to know 
#ifdef RDOC_NEVER_DEFINED
  mLibXML = rb_define_module("LibXML");
  mXML = rb_define_module_under(mLibXML, "XML");
#endif

void
ruby_init_xml_xinclude(void) {
  cXMLXInclude = rb_define_class_under(mXML, "XInclude", rb_cObject);
  eXMLXIncludeError = rb_define_class_under(cXMLXInclude, "Error", rb_eRuntimeError);
}
