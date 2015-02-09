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
      - python-psycopg2
      - python-dateutil
      - git

postgresql:
  service:
    - running

minerva-python-package:
  pip.installed:
    - name: git+https://github.com/hendrikx-itc/minerva
    - requires:
      - pkg: python-psycopg2
      - pkg: git

vagrant:
  user.present:
    - shell: /bin/zsh

  postgres_user.present:
    - login: True
    - superuser: True
    - require:
      - service: postgresql

minerva:
  postgres_database:
    - present

minerva-working-copy:
  cmd.run:
    - name: 'git clone --branch=release/5.0 https://github.com/hendrikx-itc/minerva /home/vagrant/minerva'
    - require:
      - pkg: git
    - creates: /home/vagrant/minerva

init-minerva-db:
  cmd.wait:
    - name: '/home/vagrant/minerva/schema/run-scripts /home/vagrant/minerva/schema/scripts'
    - user: vagrant
    - env:
      - PGDATABASE: minerva
    - watch:
      - postgres_database: minerva

/etc/minerva/instances/default.conf:
  file.managed:
    - source: salt://resources/minerva_instance.conf
    - makedirs: True

/etc/postgresql/9.3/main/postgresql.conf:
  file.append:
    - text: 'minerva.trigger_mark_modified = on'
