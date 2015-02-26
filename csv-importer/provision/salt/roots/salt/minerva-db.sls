include:
  - user-commands

minerva-db-packages:
  pkg:
    - installed
    - names:
      - postgresql
      - postgis
      - postgresql-9.3-postgis-scripts
      - postgresql-server-dev-9.3
      - libpq-dev
      - language-pack-nl
      - python3-psycopg2
      - python3-dateutil
      - git

postgresql:
  service:
    - running

minerva-python-package:
  pip.installed:
    - bin_env: /usr/bin/pip3
    - name: git+https://github.com/hendrikx-itc/minerva@release/5.0
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
  cmd.run:
    - name: 'git clone --branch=release/5.0 https://github.com/hendrikx-itc/minerva /home/vagrant/minerva'
    - require:
      - pkg: git
    - creates: /home/vagrant/minerva

init-minerva-db:
  cmd.wait:
    - name: '/home/vagrant/bin/create-database'
    - user: vagrant
    - watch:
      - postgres_user: vagrant
    - require:
      - sls: user-commands

/etc/minerva/instances/default.conf:
  file.managed:
    - source: salt://resources/minerva_instance.conf
    - makedirs: True

/etc/postgresql/9.3/main/postgresql.conf:
  file.append:
    - text: 'minerva.trigger_mark_modified = on'
