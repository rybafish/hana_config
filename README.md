# sap hana config
sap hana configuration changes detector

Single file python script to detect configuration changes of SAP HANA installation.

It is **not** based on m_configuration_changes. It actually compares configuration before/after and reportes what was changed.

The connection needs to be maintained to SystemDB, not tenant.

## Requirements
hdbcli for python needs to be installed/available

Proper hdbuserstore entry needs to be set up:

```hdbuserstore set <hdbstoreid> <hana_host>:<system_db_port> <user> <password>```

This &lt;hdbstoreid&gt; needs to be put into the script hana_user_key variable.

Basic configuration of the script needs to be done (first 10 lines of the script).

## Usage example
&lt;TBD&gt;
