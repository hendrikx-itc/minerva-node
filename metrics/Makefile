INSTALL_DIR = $(DESTDIR)/usr/lib/minerva/metrics

default:

install:
	install -d $(INSTALL_DIR)/
	install transform_statequeue_count.py $(INSTALL_DIR)/

install-devel:
	install -d $(INSTALL_DIR)/
	ln -s -f `pwd`/transform_statequeue_count.py $(INSTALL_DIR)

