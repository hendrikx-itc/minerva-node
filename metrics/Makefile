INSTALL_DIR = $(DESTDIR)/usr/lib/minerva/metrics

default:

install:
	install -d $(INSTALL_DIR)/
	install materialize_metrics.py $(INSTALL_DIR)/

install-devel:
	install -d $(INSTALL_DIR)/
	ln -s -f `pwd`/materialize_metrics.py $(INSTALL_DIR)

