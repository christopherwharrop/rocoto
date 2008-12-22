/* $Id: ruby_xml_xpointer.h 39 2006-02-21 20:40:16Z roscopeco $ */

/* Please see the LICENSE file for copyright and distribution information */

#ifndef __RUBY_XML_XPOINTER__
#define __RUBY_XML_XPOINTER__

extern VALUE cXMLXPointer;
extern VALUE eXMLXPointerInvalidExpression;

typedef struct ruby_xml_xpointer {
  VALUE xd;
  VALUE ctxt;
  /*
   * This needs to go into a xpointer data struct:
   *
   * xmlLocationSetPtr xptr;
   *
   * I also need an xpointer data struct type.
  */
} ruby_xml_xpointer;

VALUE ruby_xml_xpointer_point(VALUE class, VALUE node, VALUE xptr_string);
VALUE ruby_xml_xpointer_point2(VALUE node, VALUE xptr_string);
void ruby_init_xml_xpointer(void);

#endif
