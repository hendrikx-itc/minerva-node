minerva-db-packages:
  pkg:
    - installed
    - names:
      - postgresql
      - postgis
      - postgresql-9.3-postgis-scripts
      - postgresql-server-dev-9.3
      - libpq-dev
      - python-virtualenv
      - language-pack-nl
      - python3-pip
      - python3-psycopg2
      - python3-dateutil
      - git

postgresql:
  service:
    - running

minerva-python-package:
  pip.installed:
    - name: git+https://github.com/hendrikx-itc/minerva@release/5.0
    - bin_env: /usr/bin/pip3
    - requires:
      - pkg: python3-psycopg2
      - pkg: git

vagrant:
  user.present:
    - shell: /bin/zsh

  postgres_user.present:
    - login: True
    - superuser: True
    - require:
      - service: postgresql

minerva-working-copy:
  git.latest:
    - name: "https://github.com/hendrikx-itc/minerva"
    - rev: release/5.0
    - target: /home/vagrant/minerva

init-minerva-db:
  cmd.wait:
    - name: '/home/vagrant/bin/create-database'
    - user: vagrant
    - env:
      - PGDATABASE: minerva
    - watch:
      - postgres_user: vagrant

/etc/minerva/instances/default.conf:
  file.managed:
    - source: salt://resources/minerva_instance.conf
    - makedirs: True

/etc/postgresql/9.3/main/postgresql.conf:
  file.append:
    - text: 'minerva.trigger_mark_modified = on'
