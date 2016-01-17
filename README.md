# NAME

Thrift::API::HiveClient2 - Perl to HiveServer2 Thrift API wrapper

# VERSION

version 0.021

# METHODS

## new

Initialize the client object with the Hive server parameters

    my $client = Thrift::API::HiveClient2->new(
        host    => <host name or IP, defaults to localhost>,
        port    => <port, defaults to 10000>,
        timeout => <seconds timeout, defaults to 1 hour>,
    );

## connect

Open the connection on the server declared in the object's constructor.

     $client->connect() or die "Failed to connect";

## execute

Run an HiveQl statement on an open connection.

    my $rh = $client->execute( <HiveQL statement> );

## fetch

Returns an array(ref) of arrayrefs, like DBI's fetchall\_arrayref, and a boolean
indicator telling wether or not a subsequent call to fetch() will return more
rows.

    my ($rv, $has_more_rows) = $client->fetch( $rh, <maximum records to retrieve> );

IMPORTANT: The version of HiveServer2 that we use for testing is the one
bundled with CDH 4.2.1. The hasMoreRows method is currently broken, and always
returns false. So the right way of obtaining the resultset is to keep using
fetch() until it returns an empty array. For this reason the behaviour of fetch
has been altered in scalar context (which becomes the current advised way of
retrieving the data):

    # $rv will be an arrayref is anything was fetched, and undef otherwise.
    #
    while (my $rv = $client->fetch( $rh, <maximum records to retrieve> )) {
        # ... do something with @$rv
    }

This is the approach adopted in
[https://github.com/cloudera/hue/blob/master/apps/beeswax/src/beeswax/server/hive\_server2\_lib.py](https://github.com/cloudera/hue/blob/master/apps/beeswax/src/beeswax/server/hive_server2_lib.py)

Starting with version 0.12, we cache the operation handle and don't need it as
a first parameter for the fetch() call. We want to be backward-compatible
though, so depending on the type of the first parameter, we'll ignore it (since
we cached it in the object and we can get it from there) or we'll use it as the
number of rows to be retrieved if it looks like a positive integer:

     my $rv = $client->fetch( 10_000 );

## fetch\_hashref

Same use as above, but result is returned as an arrayref of hashes (which keys are
the column names)

## get\_columns

Get the columns description for a table, returned in an array of hashrefs which keys are named after the result of an
ODBC GetColumns call. "default" is used for the schema name is none is specified as 2nd argument. The hashes keys
documentation can be found on https://msdn.microsoft.com/en-us/library/ms711683(v=vs.85).aspx for instance.

    my $columns = $client->get_columns('<table name>'[, '<schema name>']);

## get\_tables

Get a list of tables. Optional table name pattern as a first argument (use undef or '%' to get all tables while
defining a schema as a second argument), and optional schema second arg (default is "default")

    my $tables = $client->get_tables(['<table pattern, SQL wildcards accepted>', ['<schema name>']]);

Returns an arrayref of hashes:

    [...
    {
        'REMARKS' => 'test comment', # table comment
        'TABLE_NAME' => 'foo_bar',   # table name
        'TABLE_SCHEM' => 'default',  # schema ("database")
        'TABLE_TYPE' => 'TABLE',     # TABLE, VIEW, etc
        'TABLE_CAT' => '',           # catalog (unused?)
    }];

# WARNING

Thrift in Perl originally did not support SASL, so authentication needed to be
disabled on HiveServer2 by setting this property in your
/etc/hive/conf/hive-site.xml. Although the property is documented, this \*value\*
\-which disables the SASL server transport- is not, AFAICT.

    <property>
      <name>hive.server2.authentication</name>
      <value>NOSASL</value>
    </property>

Starting with 0.014, support for secure clusters has been added thanks to
[Thrift::SASL::Transport](https://metacpan.org/pod/Thrift::SASL::Transport). This behaviour is set by passing sasl => 1 to the
constructor. It has been tested with hive.server2.authentication = KERBEROS.
It of course requires a valid credentials cache (kinit) or keytab.

Starting with 0.015, other authentication methods are supported, and driven by
the content of the sasl property. When built using sasl => 0 or sasl => 1, the
behaviour is unchanged. When passed a hashref of arguments that follow the
[Authen::SASL](https://metacpan.org/pod/Authen::SASL) syntax for object creation, it is passed directly to
Authen::SASL, for instance:

    {
      mechanism  => 'PLAIN',
      callback   => {
        canonuser => $USER, # not 'user', as I thought reading Authen::SASL's doc
        password  => "foobar",
      }
    }

Note that a server configured with NONE will happily accept the PLAIN method.

# CAVEATS

The instance of hiveserver2 we have didn't return results encoded in UTF8, for
the reason mentioned here:
[https://groups.google.com/a/cloudera.org/d/msg/cdh-user/AXeEuaFP0Ro/Txmn1OHleAsJ](https://groups.google.com/a/cloudera.org/d/msg/cdh-user/AXeEuaFP0Ro/Txmn1OHleAsJ)

So we had to change the init script for hive-server2 to make it behave, adding
'-Dfile.encoding=UTF-8' to HADOOP\_OPTS

# REPOSITORY

[https://github.com/dmorel/Thrift-API-HiveClient2](https://github.com/dmorel/Thrift-API-HiveClient2)

# CONTRIBUTORS

Burak Gürsoy (BURAK)

Neil Bowers (NEILB)

# AUTHOR

David Morel &lt;david.morel@amakuru.net>

# COPYRIGHT AND LICENSE

This software is Copyright (c) 2015 by David Morel & Booking.com. Portions are (c) R.Scaffidi, Thrift files are (c) Apache Software Foundation..

This is free software, licensed under:

    The Apache License, Version 2.0, January 2004
