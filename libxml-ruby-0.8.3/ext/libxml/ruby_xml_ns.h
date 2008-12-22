/* $Id: ruby_xml_ns.h 324 2008-07-08 23:00:02Z cfis $ */

/* Please see the LICENSE file for copyright and distribution information */

#ifndef __RUBY_XML_NS__
#define __RUBY_XML_NS__

extern VALUE cXMLNS;

void ruby_init_xml_ns(void);
VALUE ruby_xml_ns_wrap(xmlNsPtr ns);
#endif
