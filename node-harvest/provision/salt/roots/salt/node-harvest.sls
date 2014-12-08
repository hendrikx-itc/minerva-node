git+git://github.com/hendrikx-itc/minerva:
  pip.installed

https://github.com/hendrikx-itc/minerva-etl:
  git.latest:
    - target: /home/vagrant/minerva-etl

/home/vagrant/minerva-etl/node:
  pip.installed:
    - require:
      - git: https://github.com/hendrikx-itc/minerva-etl

/home/vagrant/minerva-etl/harvesting:
  pip.installed:
    - require:
      - git: https://github.com/hendrikx-itc/minerva-etl
      - pip: git+git://github.com/hendrikx-itc/minerva

python-nose:
  pkg.installed

node-harvest:
  pip.installed:
    - editable: /vagrant/
