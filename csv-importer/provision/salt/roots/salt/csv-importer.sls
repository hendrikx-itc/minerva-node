python-nose:
  pkg.installed

csv-importer:
  pip.installed:
    - bin_env: /usr/bin/pip3
    - editable: /vagrant/
