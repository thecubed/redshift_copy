# redshift_copy
Multiprocess-based python script to copy data from one Redshift cluster's schema to another
using the UNLOAD and COPY mechanism in Redshift.

There do indeed exist other scripts to do this exact thing, however I wrote this one so I could change configuration
as needed and use some custom SQL statements to control the columns that were unloaded/copied into the new cluster.

Mine's also multiprocess to speed up things a bit.

**Please note** this script only copies the *data* from your source DB/schema to the destination. It's up to you to copy
the table definitions.

## Requirements
`psycopg2` - Version 2.6.2 or thereabouts

## How do I use this?
Simply edit the `config.ini` file in this directory to contain your configuration values.

See the inline comments in the config file for more information.

Before running, I'd highly recommend you look over the code of `main.py` to get an idea of how the code works.

Once you're sure, just run `python main.py` and you should see some logs to show progress of the export/import.

## Help! This script did something terrible, and I'm scared :(
There shouldn't be any risk to your source database, as we only run an UNLOAD command
which is nondestructive (but resource-intensive).

The destination database/schema won't be deleted, simply appended to if it exists. Conflicting records will make the
import stop.

Essentially, this is a power-user script, only use it if you're sure you know what you're doing.