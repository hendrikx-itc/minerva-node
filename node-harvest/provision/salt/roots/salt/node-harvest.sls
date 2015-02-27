git+git://github.com/hendrikx-itc/minerva@release/5.0:
  pip.installed:
    - bin_env: /usr/bin/pip3

minerva_etl_repo:
  git.latest:
    - name: "https://github.com/hendrikx-itc/minerva-etl"
    - rev: release/5.0
    - target: /home/vagrant/minerva-etl

/home/vagrant/minerva-etl/node:
  pip.installed:
    - bin_env: /usr/bin/pip3
    - require:
      - git: minerva_etl_repo

/home/vagrant/minerva-etl/harvesting:
  pip.installed:
    - bin_env: /usr/bin/pip3
    - require:
      - git: minerva_etl_repo
      - pip: "git+git://github.com/hendrikx-itc/minerva@release/5.0"

python3-nose:
  pkg.installed

node-harvest:
  pip.installed:
    - bin_env: /usr/bin/pip3
    - editable: /vagrant/
    - require:
      - pip: /home/vagrant/minerva-etl/harvesting
