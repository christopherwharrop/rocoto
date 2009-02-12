
SHELL = /bin/sh

#### Start of system configuration section. ####

srcdir = .
topdir = /usr/lib64/ruby/1.8/x86_64-linux
hdrdir = $(topdir)
VPATH = $(srcdir):$(topdir):$(hdrdir)
prefix = $(DESTDIR)/usr
exec_prefix = $(DESTDIR)/usr
sitedir = $(DESTDIR)/usr/lib/ruby/site_ruby
rubylibdir = $(prefix)/lib/ruby/$(ruby_version)
archdir = $(libdir)/ruby/$(ruby_version)/$(arch)
sbindir = $(DESTDIR)/usr/sbin
datadir = $(DESTDIR)/usr/share
includedir = $(DESTDIR)/usr/include
infodir = $(DESTDIR)/usr/share/info
sysconfdir = $(DESTDIR)/etc
mandir = $(DESTDIR)/usr/share/man
libdir = $(DESTDIR)/usr/lib64
sharedstatedir = $(DESTDIR)/usr/com
oldincludedir = $(DESTDIR)/usr/include
sitearchdir = $(_fc_sitedir)/$(ruby_version)/$(sitearch)
bindir = $(DESTDIR)/usr/bin
localstatedir = $(DESTDIR)/var
sitelibdir = $(sitedir)/$(ruby_version)
libexecdir = $(DESTDIR)/usr/libexec
_fc_sitedir = $(DESTDIR)/usr/lib64/ruby/site_ruby

CC = gcc
LIBRUBY = $(LIBRUBY_SO)
LIBRUBY_A = lib$(RUBY_SO_NAME)-static.a
LIBRUBYARG_SHARED = -l$(RUBY_SO_NAME)
LIBRUBYARG_STATIC = -l$(RUBY_SO_NAME)-static

RUBY_EXTCONF_H = extconf.h
CFLAGS   =  -fPIC -O2 -g -pipe -Wall -Wp,-D_FORTIFY_SOURCE=2 -fexceptions -fstack-protector --param=ssp-buffer-size=4 -m64 -mtune=generic -Wall  -fPIC -I. -I/usr/lib64/ruby/1.8/x86_64-linux -I. -I/usr/include/libxml2 -I/usr/local/include 
INCFLAGS = -I. -I. -I/usr/lib64/ruby/1.8/x86_64-linux -I. -I/usr/include/libxml2 -I/usr/local/include
CPPFLAGS = -DRUBY_EXTCONF_H=\"$(RUBY_EXTCONF_H)\" 
CXXFLAGS = $(CFLAGS) 
DLDFLAGS =  -rdynamic -Wl,-export-dynamic  
LDSHARED = $(CC) -shared
AR = ar
EXEEXT = 

RUBY_INSTALL_NAME = ruby
RUBY_SO_NAME = ruby
arch = x86_64-linux
sitearch = x86_64-linux
ruby_version = 1.8
ruby = /usr/bin/ruby
RUBY = $(ruby)
RM = rm -f
MAKEDIRS = mkdir -p
INSTALL = /usr/bin/install -c
INSTALL_PROG = $(INSTALL) -m 0755
INSTALL_DATA = $(INSTALL) -m 644
COPY = cp

#### End of system configuration section. ####

preload = 

libpath = $(libdir) /usr/lib64
LIBPATH =  -L"$(libdir)" -L"/usr/lib64"
DEFFILE = 

CLEANFILES = 
DISTCLEANFILES = 

extout = 
extout_prefix = 
target_prefix = 
LOCAL_LIBS = 
LIBS = $(LIBRUBYARG_SHARED) -lxml2 -lc -lz -lm -lnsl  -lpthread -ldl -lcrypt -lm   -lc
SRCS = ruby_xml_document.c ruby_xml_ns.c ruby_xml_html_parser.c ruby_xml_node.c ruby_xml_xpath_context.c ruby_xml_parser.c ruby_xml_schema.c ruby_xml_reader.c ruby_xml_sax_parser.c ruby_xml_xpointer.c ruby_xml_state.c ruby_xml_xpath.c ruby_xml_attributes.c ruby_xml_relaxng.c ruby_xml_parser_context.c ruby_xml_node_set.c libxml.c ruby_xml_input_cbg.c cbg.c ruby_xml_attr.c ruby_xml_xpath_object.c ruby_xml_dtd.c ruby_xml_xinclude.c ruby_xml_xpointer_context.c
OBJS = ruby_xml_document.o ruby_xml_ns.o ruby_xml_html_parser.o ruby_xml_node.o ruby_xml_xpath_context.o ruby_xml_parser.o ruby_xml_schema.o ruby_xml_reader.o ruby_xml_sax_parser.o ruby_xml_xpointer.o ruby_xml_state.o ruby_xml_xpath.o ruby_xml_attributes.o ruby_xml_relaxng.o ruby_xml_parser_context.o ruby_xml_node_set.o libxml.o ruby_xml_input_cbg.o cbg.o ruby_xml_attr.o ruby_xml_xpath_object.o ruby_xml_dtd.o ruby_xml_xinclude.o ruby_xml_xpointer_context.o
TARGET = libxml_ruby
DLLIB = $(TARGET).so
EXTSTATIC = 
STATIC_LIB = 

RUBYCOMMONDIR = $(sitedir)$(target_prefix)
RUBYLIBDIR    = $(sitelibdir)$(target_prefix)
RUBYARCHDIR   = $(sitearchdir)$(target_prefix)

TARGET_SO     = $(DLLIB)
CLEANLIBS     = $(TARGET).so $(TARGET).il? $(TARGET).tds $(TARGET).map
CLEANOBJS     = *.o *.a *.s[ol] *.pdb *.exp *.bak

all:		$(DLLIB)
static:		$(STATIC_LIB)

clean:
		@-$(RM) $(CLEANLIBS) $(CLEANOBJS) $(CLEANFILES)

distclean:	clean
		@-$(RM) Makefile $(RUBY_EXTCONF_H) conftest.* mkmf.log
		@-$(RM) core ruby$(EXEEXT) *~ $(DISTCLEANFILES)

realclean:	distclean
install: install-so install-rb

install-so: $(RUBYARCHDIR)
install-so: $(RUBYARCHDIR)/$(DLLIB)
$(RUBYARCHDIR)/$(DLLIB): $(DLLIB)
	$(INSTALL_PROG) $(DLLIB) $(RUBYARCHDIR)
install-rb: pre-install-rb install-rb-default
install-rb-default: pre-install-rb-default
pre-install-rb: Makefile
pre-install-rb-default: Makefile
$(RUBYARCHDIR):
	$(MAKEDIRS) $@

site-install: site-install-so site-install-rb
site-install-so: install-so
site-install-rb: install-rb

.SUFFIXES: .c .m .cc .cxx .cpp .C .o

.cc.o:
	$(CXX) $(INCFLAGS) $(CPPFLAGS) $(CXXFLAGS) -c $<

.cxx.o:
	$(CXX) $(INCFLAGS) $(CPPFLAGS) $(CXXFLAGS) -c $<

.cpp.o:
	$(CXX) $(INCFLAGS) $(CPPFLAGS) $(CXXFLAGS) -c $<

.C.o:
	$(CXX) $(INCFLAGS) $(CPPFLAGS) $(CXXFLAGS) -c $<

.c.o:
	$(CC) $(INCFLAGS) $(CPPFLAGS) $(CFLAGS) -c $<

$(DLLIB): $(OBJS)
	@-$(RM) $@
	$(LDSHARED) $(DLDFLAGS) $(LIBPATH) -o $@ $(OBJS) $(LOCAL_LIBS) $(LIBS)



$(OBJS): ruby.h defines.h $(RUBY_EXTCONF_H)
