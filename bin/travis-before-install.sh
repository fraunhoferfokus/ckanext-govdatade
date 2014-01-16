#!/bin/sh

# Install CKAN dependencies
sudo apt-get update -q
sudo apt-get install solr-jetty
pip install -e 'git+https://github.com/okfn/ckan.git@ckan-1.8#egg=ckan'
pip install -r ~/virtualenv/python2.7/src/ckan/pip-requirements.txt

# Configure Solr
echo "NO_START=0\nJETTY_HOST=127.0.0.1\nJETTY_PORT=8983\nJAVA_HOME=$JAVA_HOME" | sudo tee /etc/default/jetty
sudo cp ~/virtualenv/python2.7/src/ckan/ckan/config/solr/schema-1.4.xml /etc/solr/conf/schema.xml
sudo service jetty restart

# Setup PostgreSQL database
sudo -u postgres psql -c "CREATE USER harvest WITH PASSWORD 'pass';"
sudo -u postgres psql -c 'CREATE DATABASE ckan_test WITH OWNER harvest;'

# Create CKAN configuration file
cd ~/virtualenv/python2.7/src/ckan
paster make-config ckan development.ini
sed -i 's,# ckan.site_id,ckan.site_id,' development.ini
sed -i 's,#solr_url,solr_url,' development.ini
paster --plugin=ckan db init

# Change to project directory again
cd ~/build/fraunhoferfokus/ckanext-govdatade
python setup.py develop
